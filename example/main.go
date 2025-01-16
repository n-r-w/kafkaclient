//nolint:mnd,forbidigo // example
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	kafkaconsumer "github.com/n-r-w/kafkaclient/consumer"
	kafkaproducer "github.com/n-r-w/kafkaclient/producer"
	"github.com/testcontainers/testcontainers-go/modules/kafka"
)

// Message represents our business domain message structure.
type Message struct {
	ID        string    `json:"id"`
	Timestamp time.Time `json:"timestamp"`
	Payload   string    `json:"payload"`
}

// MessageProcessor implements IConsumeProcessor interface.
type MessageProcessor struct{}

func (p *MessageProcessor) ConsumeProcessorName() string {
	return "message-processor"
}

func (p *MessageProcessor) ConsumeKafkaMessages(
	ctx context.Context,
	topic string,
	partition int32,
	msgs []kafkaconsumer.IMessage,
) error {
	fmt.Printf("Start processing batch of %d messages\n", len(msgs))

	for _, msg := range msgs {
		var message Message
		if err := msg.ReadInJSON(&message); err != nil {
			return fmt.Errorf("failed to parse message: %w", err)
		}

		// Process the message
		fmt.Printf("Processing message: %+v\n", message)

		// Here you would add your business logic
		// For example: save to database, call external service, etc.
	}
	return nil
}

func (p *MessageProcessor) ConsumeProcessorStop() {
	// Cleanup resources if needed
	fmt.Println("Message processor stopped")
}

func runConsumer(ctx context.Context, brokers []string, groupID, topic string) (*kafkaconsumer.Consumer, error) {
	// Create message processor
	processor := &MessageProcessor{}

	// Create consumer with configuration
	c, err := kafkaconsumer.New(
		ctx,
		"example-service",
		brokers,
		groupID,
		map[kafkaconsumer.IConsumeProcessor][]string{
			processor: {topic},
		},
		kafkaconsumer.WithBatchTopics(topic),
		kafkaconsumer.WithBatchSize(5),
		kafkaconsumer.WithFlushTimeout(2*time.Second),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}

	// Start consumer
	if err := c.Start(ctx); err != nil {
		return nil, fmt.Errorf("failed to start consumer: %w", err)
	}
	log.Println("Consumer started")
	return c, nil
}

// createMessage generates a new Kafka message with the given index and type.
func createMessage(topic string, index int, msgType string) (*sarama.ProducerMessage, error) {
	time.Sleep(100 * time.Millisecond) // Throttle message sending

	message := Message{
		ID:        fmt.Sprintf("msg-%d", index),
		Timestamp: time.Now(),
		Payload:   fmt.Sprintf("%s message payload %d", msgType, index),
	}

	jsonData, err := json.Marshal(message)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal message: %w", err)
	}

	return &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(jsonData),
	}, nil
}

func runSyncProducer(ctx context.Context, brokers []string, topic string) (*kafkaproducer.SyncProducer, error) {
	// Create and start producer
	producer, err := kafkaproducer.NewSyncProducer(ctx, "example-service", brokers)
	if err != nil {
		return nil, fmt.Errorf("failed to create producer: %w", err)
	}

	if err := producer.Start(ctx); err != nil {
		return nil, fmt.Errorf("failed to start producer: %w", err)
	}
	log.Println("Sync producer started")

	// Send messages
	go func() {
		for i := 0; ; i++ {
			msg, err := createMessage(topic, i, "sync")
			if err != nil {
				log.Printf("Failed to create message: %v", err)
				continue
			}
			if _, _, err := producer.SendMessage(ctx, msg); err != nil {
				log.Printf("Failed to send message: %v", err)
			}
		}
	}()

	return producer, nil
}

func runAsyncProducer(ctx context.Context, brokers []string, topic string) (*kafkaproducer.AsyncProducer, error) {
	// Create and start producer
	producer, err := kafkaproducer.NewAsyncProducer(ctx, "example-service", brokers)
	if err != nil {
		return nil, fmt.Errorf("failed to create producer: %w", err)
	}

	if err := producer.Start(ctx); err != nil {
		return nil, fmt.Errorf("failed to start producer: %w", err)
	}
	log.Println("Async producer started")

	// Send messages
	go func() {
		for i := 0; ; i++ {
			msg, err := createMessage(topic, i, "async")
			if err != nil {
				log.Printf("Failed to create message: %v", err)
				continue
			}
			producer.SendMessage(ctx, msg)
		}
	}()

	return producer, nil
}

func main() {
	ctx := context.Background()

	// Start Kafka container
	log.Println("Starting Kafka container...")
	container, err := kafka.Run(ctx,
		"confluentinc/cp-kafka:7.5.0",
		kafka.WithClusterID("example-cluster"),
	)
	if err != nil {
		log.Printf("Failed to start Kafka container: %v", err)
		return
	}
	log.Println("Kafka container started")

	brokers, err := container.Brokers(ctx)
	if err != nil {
		log.Printf("Failed to get Kafka brokers: %v", err)
		return
	}

	// Configuration
	groupID := "example-consumer-group"
	topic := "example-topic"

	// Create topic
	config := sarama.NewConfig()
	config.Version = sarama.V3_5_0_0

	admin, err := sarama.NewClusterAdmin(brokers, config)
	if err != nil {
		log.Printf("Failed to create cluster admin: %v", err)
		return
	}
	defer func() { _ = admin.Close() }()

	err = admin.CreateTopic(topic, &sarama.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}, false)
	if err != nil {
		log.Printf("Failed to create topic: %v", err)
		return
	}

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	// Run consumer
	consumer, err := runConsumer(ctx, brokers, groupID, topic)
	if err != nil {
		log.Printf("Failed to start consumer: %v", err)
		return
	}

	// Run sync producer
	syncProducer, err := runSyncProducer(ctx, brokers, topic)
	if err != nil {
		log.Printf("Failed to start sync producer: %v", err)
		return
	}

	// Run async producer
	asyncProducer, err := runAsyncProducer(ctx, brokers, topic)
	if err != nil {
		log.Printf("Failed to start async producer: %v", err)
		return
	}

	// Wait for shutdown signal
	<-sigchan
	log.Println("Shutting down...")

	// Stop consumer and producers
	if err := consumer.Stop(context.Background()); err != nil {
		log.Printf("Error stopping consumer: %v", err)
	}
	log.Println("Consumer stopped")

	if err := syncProducer.Stop(context.Background()); err != nil {
		log.Printf("Error stopping sync producer: %v", err)
	}
	log.Println("Sync producer stopped")

	if err := asyncProducer.Stop(context.Background()); err != nil {
		log.Printf("Error stopping async producer: %v", err)
	}
	log.Println("Async producer stopped")
}
