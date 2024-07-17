package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"time"

	"github.com/IBM/sarama"
)

const (
	kafkaTopic  = "user-locations"
	kafkaBroker = "localhost:9092"
)

type Location struct {
	UserID    string    `json:"user_id"`
	Lat       float64   `json:"latitude"`
	Lon       float64   `json:"longitude"`
	Timestamp time.Time `json:"timestamp"`
}

func main() {
	if len(os.Args) != 2 {
		fmt.Println("Please specify 'producer' or 'consumer' as an argument")
		os.Exit(1)
	}

	switch os.Args[1] {
	case "producer":
		runProducer()
	case "consumer":
		runConsumer()
	default:
		fmt.Println("Invalid argument. Use 'producer' or 'consumer'")
		os.Exit(1)
	}
}

func runProducer() {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer([]string{kafkaBroker}, config)
	if err != nil {
		log.Fatalf("Failed to create producer: %s", err)
	}
	defer producer.Close()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	for {
		select {
		case <-signals:
			return
		default:
			location := generateMockLocation()
			sendLocationUpdate(producer, location)
			time.Sleep(5 * time.Second)
		}
	}
}

func generateMockLocation() Location {
	return Location{
		UserID:    fmt.Sprintf("user_%d", rand.Intn(1000)),
		Lat:       rand.Float64()*180 - 90,  // Range: -90 to 90
		Lon:       rand.Float64()*360 - 180, // Range: -180 to 180
		Timestamp: time.Now(),
	}
}

func sendLocationUpdate(producer sarama.SyncProducer, location Location) {
	payload, err := json.Marshal(location)
	if err != nil {
		log.Printf("Failed to marshal location: %s", err)
		return
	}

	msg := &sarama.ProducerMessage{
		Topic: kafkaTopic,
		Value: sarama.StringEncoder(payload),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		log.Printf("Failed to send message: %s", err)
	} else {
		log.Printf("Message sent to partition %d at offset %d", partition, offset)
	}
}

func runConsumer() {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	consumer, err := sarama.NewConsumer([]string{kafkaBroker}, config)
	if err != nil {
		log.Fatalf("Failed to create consumer: %s", err)
	}
	defer consumer.Close()

	partitionConsumer, err := consumer.ConsumePartition(kafkaTopic, 0, sarama.OffsetNewest)
	if err != nil {
		log.Fatalf("Failed to create partition consumer: %s", err)
	}
	defer partitionConsumer.Close()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	for {
		select {
		case msg := <-partitionConsumer.Messages():
			var location Location
			err := json.Unmarshal(msg.Value, &location)
			if err != nil {
				log.Printf("Failed to unmarshal message: %s", err)
				continue
			}
			log.Printf("Received location update: User %s at (%.6f, %.6f) at %s",
				location.UserID, location.Lat, location.Lon, location.Timestamp)
		case err := <-partitionConsumer.Errors():
			log.Printf("Error: %s", err.Error())
		case <-signals:
			return
		}
	}
}
