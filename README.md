
# Real-Time Payment Fraud Stream Processor

A distributed stream-processing pipeline that ingests a high-volume payment event stream,
computes rolling per-user metrics, and fires fraud alerts — built to remain correct under
duplicates, corrupt data, out-of-order events, and consumer failures.

The emphasis of this project is **correctness under failure**, not the fraud logic itself.
The fraud rules are deliberately simple (e.g. "≥3 failed payments for a user in 5 minutes");
the engineering effort went into the stream-processing guarantees around them.

---

## What it does

A load generator produces `PaymentEvent`s (keyed by user) into Kafka. A group of consumers
reads the stream, maintains rolling per-user aggregates in Redis, and emits an alert when a
user crosses a failure threshold within a sliding time window. Corrupt messages are routed to
a dead-letter queue; transient failures are retried; late and out-of-order events are handled
via event-time watermarks; and the consumer group survives rebalances and crashes without
double-counting.

---

## Architecture

```
                 ┌──────────────┐
   Load          │              │   keyed by user_id
   Generator ───▶│    Kafka     │──────────────────────┐
   (producer)    │  (3 parts)   │                       │
                 └──────────────┘                       ▼
                        │                    ┌─────────────────────┐
                        │                    │  Consumer Group     │
                        │                    │  (≤3 consumers,     │
                        │                    │   1 partition each) │
                        │                    └─────────┬───────────┘
                        │                              │
                 ┌──────▼───────┐              ┌───────▼────────┐
                 │  DLQ topic   │◀─────────────│     Redis      │
                 │ (poison      │  bad msgs    │ (derived state:│
                 │  pills)      │              │  windows,      │
                 └──────────────┘              │  spend totals) │
                                               └────────────────┘
```

- **Producer** — emits randomized payment events, keyed by `user_id` so all of one user's
  events land on the same partition (preserving per-user ordering). Async batched production.
  Supports fault-injection knobs (see below) for reproducible testing of each failure mode.
- **Kafka** — a 3-partition topic acting as the durable, replicated, ordered log. Runs in
  KRaft mode (no ZooKeeper) via Docker. The partition count sets the maximum consumer parallelism.
- **Consumer group** — up to 3 consumers, one partition each. Reads events, updates Redis,
  fires alerts. Uses manual offset commits for correctness.
- **Redis** — holds *derived* state only (rolling windows and running totals), never the events
  themselves. Kafka is the system of record; Redis is the fast-access scoreboard.
- **DLQ topic** — quarantines messages that can never be processed (corrupt payloads).

### Event schema

```
PaymentEvent {
  event_id     string   // unique per event (UUID) — dedup key
  user_id      string   // partition key; drawn from a stable pool (user_000..user_099)
  amount_cents int64    // money as integer minor units (never float)
  merchant     string
  status       string   // "SUCCESS" | "FAILURE"
  event_time   int64    // unix millis — when it HAPPENED (distinct from arrival)
}
```

The `PaymentEvent` type lives in a shared `events` package imported by both producer and
consumer, so the wire contract has a single definition.

---

## Tech stack

- **Go** — producer and consumer
- **Apache Kafka** (KRaft mode) — the event log, in Docker
- **Redis** — derived state store
- **[franz-go](https://github.com/twmb/franz-go)** — Kafka client
- **[go-redis](https://github.com/redis/go-redis)** — Redis client, with atomic Lua scripts
- **[cenkalti/backoff](https://github.com/cenkalti/backoff)** — bounded exponential backoff for retries
- **Docker Compose** — Kafka broker

---

## Core engineering challenges

### 1. Effectively-once processing

With at-least-once delivery, an event can be processed more than once (a consumer crashes
after updating Redis but before committing its offset; the replacement reprocesses it). The
naive additive spend update (`INCRBY`) double-counts under reprocessing.

**Solution.** Two coordinated pieces:

- **Manual offset commits.** Auto-commit is disabled; offsets are committed *after* processing
  a batch. This gives at-least-once delivery (a crash can only cause reprocessing, never lost
  data), rather than the auto-commit default which can commit records before they are processed.
- **Idempotency, matched to each state type:**
  - *Spend total* uses an **atomic Lua script** that checks a `processed:<event_id>` marker,
    and only if absent applies the `INCRBY` and sets the marker — all in one indivisible
    operation, so a crash cannot split the check from the apply. Markers carry a TTL.
  - *Failure window* is **idempotent by construction**: it stores each failure in a Redis
    sorted set keyed by `event_id`, so re-adding the same event is a harmless overwrite. No
    dedup marker needed.

**Guarantee.** *Effectively-once via at-least-once delivery + Lua-atomic dedup on `event_id`
(1h TTL) for the spend path, with failure aggregates idempotent by construction.* Verified by
killing a consumer mid-stream and confirming the spend total is unchanged by the reprocessing.

### 2. Poison pills, retries & dead-letter queue

A single corrupt payload must not block a partition, and a transient dependency failure must
not be mistaken for bad data.

**Solution.** An error classifier distinguishes three cases:

- **Bad message** (`json.SyntaxError`) → the payload is unrecoverable; the *original bytes* are
  routed to the DLQ and the consumer moves on. (The raw bytes are preserved, not a re-marshaled
  empty struct, so the DLQ is actually inspectable.)
- **Transient** (network / `context.DeadlineExceeded` / connection failures) → retried inline
  with **bounded exponential backoff**. Retry is safe precisely because the operations are
  idempotent.
- **Permanent** (command/script errors) → logged; not retried (retrying repeats the rejection).

The two failure sources are handled with different strategies: a bad *message* is quarantined
individually, whereas a *transient dependency failure* is retried rather than discarded — a
good event hitting a down Redis is not a poison pill.

### 3. Event-time windowing & out-of-order events

Events arrive out of order (network latency, retries, rebalancing), so a naive window keyed on
wall-clock — or on the current event's own timestamp — gives wrong counts. Using the current
event's `event_time` as the eviction reference is non-monotonic: a late event with an old
timestamp drags the eviction cutoff *backward*, so window membership flaps depending on arrival
order.

**Solution.** A **watermark**: a monotonic estimate of how far event-time has advanced.

- Tracked as the **maximum `event_time` per partition** (a late event cannot lower a
  partition's max, so the watermark never regresses).
- The stream watermark is the **minimum across partitions** — event-time has only truly
  advanced everywhere to the extent the *slowest* partition has advanced.
- Eviction uses `watermark − window − allowedLateness` as the cutoff, a stable monotonic
  reference rather than the current event's time.
- During startup (before the watermark is established) eviction and alerting are deferred
  rather than run against an untrustworthy reference.

**Tradeoff (deliberate).** Events arriving below the cutoff (genuinely too late) are **silently
dropped** rather than triggering window revision. Near-real-time fraud detection favors
*timeliness over completeness* — waiting for perfect completeness would delay every alert.
This is the completeness-vs-latency tradeoff resolved in favor of latency.

### 4. Consumer groups, rebalancing & distributed state

Running multiple consumers exposes problems a single consumer never faces. In particular, the
watermark logic — which assumed one consumer sees all partitions — breaks under distribution:
each consumer sees only its assigned partition(s).

**Solution.**

- **Readiness over owned partitions**, not a hardcoded partition count. A consumer establishes
  its watermark over the partition(s) it actually owns. This is correct here because state is
  partitioned by user key — each user is wholly owned by one consumer, so a per-consumer local
  watermark is sufficient for that consumer's users.
- **Rebalance-aware cleanup.** An `OnPartitionsRevoked` callback deletes revoked partitions
  from the watermark map, so a partition a consumer no longer owns does not freeze its
  watermark with a stale, non-advancing value.
- **Concurrency safety.** The rebalance callback runs on a separate goroutine from the poll
  loop; a mutex guards all access to the shared watermark map.
- **Effectively-once survives rebalance.** When a consumer crashes and its partition is
  reassigned, the new owner reprocesses uncommitted events — made harmless by the idempotency
  in challenge #1.

---

## Known limitations

Stated explicitly, because the guarantees are only meaningful alongside their boundaries:

- **Sustained Redis outage drops events.** Transient errors are retried with bounded backoff,
  which covers brief blips. A Redis outage lasting beyond the retry budget causes affected
  events to exhaust retries and be committed past (lost). A production system would *pause
  consumption* until Redis recovers rather than committing past failed events.
- **Idle partitions stall the watermark.** The stream watermark is the min across partitions,
  so a partition that goes fully silent freezes the watermark (its max never advances). Real
  systems handle this with an idle-partition timeout that excludes silent partitions.
- **The watermark is in-memory** and is rebuilt from incoming events after a consumer restart.
- **No true global watermark under distribution.** Each consumer watermarks only over its own
  partitions; there is no cross-partition global watermark. This is acceptable *because* state
  is partitioned by user key (no user's data spans partitions), but it would not suffice for
  aggregations that cross partition boundaries.
- **Batch offset commits** widen the reprocessing window (a crash mid-batch replays the whole
  batch), which is safe only because processing is idempotent.

---

## Running it

### Prerequisites

- Docker (for Kafka)
- A running Redis (e.g. `docker run -d -p 6379:6379 --name my-redis redis`)
- Go

### 1. Start Kafka

```bash
docker compose up -d
```

### 2. Create the topics

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --topic payment-events-topic \
  --partitions 3 --replication-factor 1

docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --topic events-dlq-topic \
  --partitions 3 --replication-factor 1
```

### 3. Run the producer

```bash
cd producers
go run . -rate 100                    # 100 events/sec
go run . -rate 100 -corruption 10     # + 10% malformed payloads (tests the DLQ)
go run . -rate 100 -clock-skew 10     # + 10% backdated events (tests event-time windowing)
```

### 4. Run the consumer(s)

```bash
cd consumers
go run .          # start one; start up to 3 in separate terminals for the consumer group
```

### Producer fault-injection knobs

| Flag            | Effect                                   | Exercises          |
| --------------- | ---------------------------------------- | ------------------ |
| `-rate`       | events per second                        | throughput / lag   |
| `-corruption` | % of events emitted as malformed JSON    | poison pills / DLQ |
| `-clock-skew` | % of events with backdated`event_time` | event-time windows |

### Useful inspection commands

```bash
# consumer group state, partition assignment, and lag
docker exec -it kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group my-group-identifier

# inspect the DLQ
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic events-dlq-topic --from-beginning

# watch a user's rolling state
docker exec -it my-redis redis-cli GET user:user_042:total_spend_today
docker exec -it my-redis redis-cli ZRANGE user:user_042:failures 0 -1 WITHSCORES
```

---

## Project structure

```
.
├── docker-compose.yml      # single-broker Kafka (KRaft)
├── events/
│   └── event.go            # shared PaymentEvent wire contract
├── producers/
│   └── main.go             # load generator with fault-injection knobs
└── consumers/
    └── main.go             # stateful consumer: dedup, DLQ, watermarks, rebalance handling
```


