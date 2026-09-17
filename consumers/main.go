package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	// "time"
	// "strconv"
	"errors"
	"net"
	"github.com/cenkalti/backoff/v7"

	// "time"
	"kafka-go-start/events"

	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"
)

var spendScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 1 then
	return 0
end
redis.call('INCRBY', KEYS[2], ARGV[1])
redis.call('SET', KEYS[1], 1, 'EX', ARGV[2])
return 1
`)

var failureScript = redis.NewScript(`
redis.call('ZADD', KEYS[1], ARGV[2], ARGV[1])
redis.call('ZREMRANGEBYSCORE', KEYS[1], '0', ARGV[3])
redis.call('EXPIRE', KEYS[1], ARGV[4])
return redis.call('ZCARD', KEYS[1]) 
`)

func classify(err error) string {   // or keep bool isTransient for now
    if err == nil {
        return "ok"
    }
    var syntaxErr *json.SyntaxError
    if errors.As(err, &syntaxErr) {
        return "bad_message"   // → DLQ
    }
    if errors.Is(err, context.DeadlineExceeded) {
        return "transient"     // → retry
    }
    var netErr net.Error       // FIXED: interface, not *interface
    if errors.As(err, &netErr) {
        return "transient"     // → retry
    }
    return "permanent"         // → log/alert (code or config problem)
}

func main() {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379", // Redis server address
		Password: "",               // No password by default
		DB:       0,                // Default database
	})
	ctx := context.Background()

	opts := []kgo.Opt{
		kgo.SeedBrokers("localhost:9092"),
		kgo.ClientID("consumer-client-id"),
		kgo.ConsumerGroup("my-group-identifier"),
		kgo.ConsumeTopics("payment-events-topic"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	}
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return
	}

	defer client.Close()

	// ticker := time.NewTicker(5 * time.Second)
	for {
		fetches := client.PollFetches(context.Background())
		if errs := fetches.Errors(); len(errs) > 0 {
			log.Printf("Error in fetching: %v", errs)
			continue
		}

		fetches.EachRecord(func(r *kgo.Record) {
			var ev events.PaymentEvent

	
			eventInfo := r.Value
			if err := json.Unmarshal(eventInfo, &ev); err != nil {
				if classify(err) == "bad_message" {
					dlq_record := &kgo.Record{
						Key: r.Key,
						Value: r.Value,
						Topic: "events-dlq-topic",
					}
					if err := client.ProduceSync(ctx, dlq_record).FirstErr(); err != nil {
						log.Printf("failed to write to DLQ (offset %d): %v", r.Offset, err)
						// don't return/commit past it — let it be reprocessed rather than lost
						return
					}
				}
				// log.Printf("Error due to %v", err)
				return
			}
			eventIdempotencyKey := fmt.Sprintf("processed:%v", ev.EventID)
			// ok, _ := rdb.SetNX(ctx, eventIdempotencyKey, 1, time.Hour).Result()
			// if !ok {
			// 	return 
			// }
			userId := ev.UserID
			baseKey := fmt.Sprintf("user:%v:", userId)
			amountKey := baseKey + "total_spend_today"
			// failureKey := baseKey + "failed_count_today"
			failureTimestampKey := baseKey + "failures"
			// status := 0
			fiveMinutesAgo := ev.EventTime - 5*60*1000
			
			if ev.Status == "FAILURE" {
				count, err := backoff.Retry(ctx, func() (int64, error) {
					n, err := failureScript.Run(ctx, rdb,
						[]string{failureTimestampKey},
						ev.EventID, ev.EventTime, fiveMinutesAgo, 600).Int64()
					if err != nil {
						if classify(err) == "transient" { return 0, err }
						return 0, backoff.Permanent(err)
					}
					return n, nil
				}, backoff.WithBackOff(backoff.NewExponentialBackOff()), backoff.WithMaxTries(5))
					
				if err != nil {
					log.Printf("failure script failed for %s: %v", ev.EventID, err)
					return
				}
				if count >= 3 {
					log.Printf("ALERT: user %s had %d failed payments in last 5 min", userId, count)
				}
			} else {
				_, err := backoff.Retry(ctx, func() (int64, error) {
					n, err := spendScript.Run(ctx, rdb,
						[]string{eventIdempotencyKey, amountKey},
						ev.AmountCents, 3600).Int64()
					if err != nil {
						if classify(err) == "transient" { return 0, err }
						return 0, backoff.Permanent(err)
					}
					return n, nil
				}, backoff.WithBackOff(backoff.NewExponentialBackOff()), backoff.WithMaxTries(5))
				if err != nil {
					log.Printf("spend failed after retries for %s: %v", ev.EventID, err)
					return
				}
			}
		})
		if err := client.CommitRecords(ctx, fetches.Records()...); err != nil {
			log.Printf("Record commit failed: %v", err)
		}
	}
}