package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"math/rand"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
)

type PaymentEvent struct {
	EventID string `json:"event_id"`
	UserID string `json:"user_id"`
	AmountCents int64 `json:"amount_cents"`
	Merchant string `json:"merchant"`
	Status string `json:"status"`
	EventTime int64 `json:"event_time"`
}

func randomize_payment_event(user_pool []string) PaymentEvent {
	event_id := uuid.New().String()
	user_id := user_pool[rand.Intn(len(user_pool))]
	amount := int64(rand.Intn(50000) + 1)
	merchant := "stripe"
	temp := rand.Int() % 2
	status := "SUCCESS"
	if temp == 1 {
		status = "FAILURE"
	}
	event_time := time.Now().UnixMilli()
	return PaymentEvent{event_id, user_id, amount, merchant, status, event_time}
}



func main() {
	rate       := flag.Int("rate", 100, "events per second")
	// corruption := flag.Int("corruption", 0, "percent of events emitted as malformed JSON (0-100)")
	// clockSkew  := flag.Bool("clock-skew", false, "randomly backdate some event_time values")
	// dupRate    := flag.Int("dup-rate", 0, "percent of events re-emitted as duplicates (0-100)")
	flag.Parse()

	var userPool []string
	for i := 0; i < 100; i++ {
		userPool = append(userPool, uuid.New().String())
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers("localhost:9092"),
		kgo.DefaultProduceTopic("payment-events-topic"),
		kgo.ClientID("my-client-id"),
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		log.Fatalf("Creating client, %v", err)
	}
	defer client.Close()

	
	interval := time.Second / time.Duration(*rate)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	ctx := context.Background()

	for range ticker.C {
		event := randomize_payment_event(userPool)
	
		payload, err := json.Marshal(event)
		if err != nil {
			log.Fatalf("Failed to encode payment event!")
		}

		record := &kgo.Record{
			Key: []byte(event.UserID),
			Value: payload,
			Topic: "payment-events-topic",
		}
		client.Produce(ctx, record, func(r *kgo.Record, err error) {
			if err != nil {
				log.Printf("Error in producing this record, %v", err)
			}
		})
	}

	// if err := client.ProduceSync(context.Background(), record).FirstErr(); err != nil {
	// 	log.Fatalf("Sending record, %v", err)
	// }
}

