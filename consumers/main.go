package main

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
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

	partitionCounts := map[int32]int{}          // messages per partition
	usersByPartition := map[int32]map[string]bool{}  // distinct users per partition

	defer client.Close()

	ticker := time.NewTicker(5 * time.Second)
	for {
		fetches := client.PollFetches(context.Background())
		if errs := fetches.Errors(); len(errs) > 0 {
			panic(fmt.Sprint(errs))
		}

		fetches.EachRecord(func(r *kgo.Record) {	
			p := r.Partition
			partitionCounts[p]++
			if usersByPartition[p] == nil {
				usersByPartition[p] = map[string]bool{}
			}
			usersByPartition[p][string(r.Key)] = true
		})

		for range ticker.C {
			fmt.Println(partitionCounts)
			fmt.Println(usersByPartition)

		}
// every few seconds, print the maps
	}
}