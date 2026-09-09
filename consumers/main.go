package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"
	"strconv"

	// "time"
	"kafka-go-start/events"

	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"
)


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
	}
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return
	}

	// partitionCounts := map[int32]int{}          // messages per partition
	// usersByPartition := map[int32]map[string]bool{}  // distinct users per partition

	defer client.Close()

	// ticker := time.NewTicker(5 * time.Second)
	for {
		fetches := client.PollFetches(context.Background())
		if errs := fetches.Errors(); len(errs) > 0 {
			panic(fmt.Sprint(errs))
		}

		fetches.EachRecord(func(r *kgo.Record) {
			var ev events.PaymentEvent

	
			eventInfo := r.Value
			if err := json.Unmarshal(eventInfo, &ev); err != nil {
				log.Printf("Error due to %v", err)
				return
			}
			userId := ev.UserID
			baseKey := fmt.Sprintf("user:%v:", userId)
			amountKey := baseKey + "total_spend_today"
			// failureKey := baseKey + "failed_count_today"
			failureTimestampKey := baseKey + "failures"
			// status := 0
			fiveMinutesAgo := ev.EventTime - 5*60*1000

			if ev.Status == "FAILURE" {
				rdb.ZAdd(ctx, failureTimestampKey, redis.Z{Member: ev.EventID, Score: float64(ev.EventTime)})
				// status = 1
				rdb.ZRemRangeByScore(ctx, failureTimestampKey, redis.NInf, strconv.FormatInt(fiveMinutesAgo, 10))
				if failureEvents, err := rdb.ZCard(ctx, failureTimestampKey).Uint64(); failureEvents >= 3 && err == nil {
					log.Printf("ALERT: user %s had %d failed payments in last 5 min", userId, failureEvents)
				}
				rdb.Expire(ctx, failureTimestampKey, 10 * time.Minute)
			} else {
				rdb.IncrBy(ctx, amountKey, ev.AmountCents)
			}
			
			// rdb.IncrBy(ctx, failureKey, int64(status))


			// p := r.Partition
			// partitionCounts[p]++
			// if usersByPartition[p] == nil {
			// 	usersByPartition[p] = map[string]bool{}
			// }
			// usersByPartition[p][string(r.Key)] = true
		})

		// for range ticker.C {
		// 	fmt.Println(partitionCounts)
		// 	fmt.Println(usersByPartition)

		// }
// every few seconds, print the maps
	}
}