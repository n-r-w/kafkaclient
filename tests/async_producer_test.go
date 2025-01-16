package realtest

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/IBM/sarama"
	kafkaproducer "github.com/n-r-w/kafkaclient/producer"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestAsyncProducer_Integration(t *testing.T) {
	const (
		testTopic     = "test-topic-async"
		testMessage   = "test-message"
		serviceName   = "test-service"
		testPartition = int32(0)
	)

	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Setup mock controller.
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Create mock implementations.
	mockTelemetry := kafkaproducer.NewMockITelemetry(ctrl)
	mockErrorLogger := kafkaproducer.NewMockIErrorLogger(ctrl)

	// Setup expectations.
	mockTelemetry.EXPECT().
		CollectWriteLatency(gomock.Any(), serviceName, gomock.Any(), testTopic, gomock.Any(), gomock.Any(), gomock.Any(), true).
		AnyTimes()
	mockTelemetry.EXPECT().
		CollectWriteSize(gomock.Any(), serviceName, gomock.Any(), testTopic, gomock.Any(), gomock.Any(), gomock.Any(), true).
		AnyTimes()
	mockErrorLogger.EXPECT().
		LogError(gomock.Any(), gomock.Any()).
		AnyTimes()

	// Setup Kafka container.
	kafka := setupKafka(t)
	defer kafka.cleanup(t)

	// Create test topic.
	kafka.createTopic(t, testTopic, 1)

	t.Run("producer initialization", func(t *testing.T) {
		ctx := context.Background()
		producer, err := kafkaproducer.NewAsyncProducer(ctx, serviceName, kafka.brokers,
			kafkaproducer.WithTelemetry(mockTelemetry),
			kafkaproducer.WithErrorLogger(mockErrorLogger),
		)
		require.NoError(t, err, "Failed to create producer")
		defer func() {
			err = producer.Stop(ctx)
			require.NoError(t, err, "Failed to stop producer")
		}()

		err = producer.Start(ctx)
		require.NoError(t, err, "Failed to start producer")
	})

	t.Run("send single message", func(t *testing.T) {
		ctx := context.Background()
		producer, err := kafkaproducer.NewAsyncProducer(ctx, serviceName, kafka.brokers,
			kafkaproducer.WithTelemetry(mockTelemetry),
			kafkaproducer.WithErrorLogger(mockErrorLogger),
		)
		require.NoError(t, err, "Failed to create producer")

		err = producer.Start(ctx)
		require.NoError(t, err, "Failed to start producer")
		defer func() {
			err = producer.Stop(ctx)
			require.NoError(t, err, "Failed to stop producer")
		}()

		msg := &sarama.ProducerMessage{
			Topic: testTopic,
			Value: sarama.StringEncoder(testMessage),
		}

		producer.SendMessage(ctx, msg)

		// Verify the message was sent by consuming it.
		consumer := createTestConsumer(t, kafka.brokers)
		defer func() { _ = consumer.Close() }()

		partitionConsumer, err := consumer.ConsumePartition(testTopic, testPartition, 0)
		require.NoError(t, err, "Failed to create partition consumer")
		defer func() { _ = partitionConsumer.Close() }()

		select {
		case msg := <-partitionConsumer.Messages():
			require.Equal(t, testMessage, string(msg.Value), "Unexpected message content")
		case <-time.After(5 * time.Second):
			t.Fatal("Timeout waiting for message")
		}
	})

	t.Run("send multiple messages", func(t *testing.T) {
		ctx := context.Background()
		producer, err := kafkaproducer.NewAsyncProducer(ctx, serviceName, kafka.brokers,
			kafkaproducer.WithTelemetry(mockTelemetry),
			kafkaproducer.WithErrorLogger(mockErrorLogger),
		)
		require.NoError(t, err, "Failed to create producer")

		err = producer.Start(ctx)
		require.NoError(t, err, "Failed to start producer")
		defer func() {
			err = producer.Stop(ctx)
			require.NoError(t, err, "Failed to stop producer")
		}()

		messages := make([]*sarama.ProducerMessage, 3)
		for i := range messages {
			messages[i] = &sarama.ProducerMessage{
				Topic: testTopic,
				Value: sarama.StringEncoder(fmt.Sprintf("%s-%d", testMessage, i)),
			}
		}

		producer.SendMessages(ctx, messages)

		// Verify messages were sent by consuming them.
		consumer := createTestConsumer(t, kafka.brokers)
		defer func() { _ = consumer.Close() }()

		partitionConsumer, err := consumer.ConsumePartition(testTopic, testPartition, 1) // offset 1 since we already consumed message from previous test
		require.NoError(t, err, "Failed to create partition consumer")
		defer func() { _ = partitionConsumer.Close() }()

		receivedCount := 0
		for receivedCount < len(messages) {
			select {
			case msg := <-partitionConsumer.Messages():
				require.Equal(t, fmt.Sprintf("%s-%d", testMessage, receivedCount), string(msg.Value),
					"Unexpected message content")
				receivedCount++
			case <-time.After(5 * time.Second):
				t.Fatal("Timeout waiting for messages")
			}
		}
	})
}
