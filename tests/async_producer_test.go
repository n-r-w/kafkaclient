package realtest

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/IBM/sarama"
	producer "github.com/n-r-w/kafkaclient/producer"
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
	mockTelemetry := producer.NewMockITelemetry(ctrl)
	mockErrorLogger := producer.NewMockIErrorLogger(ctrl)

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

	// Verify the message was sent by consuming it.
	consumer := createTestConsumer(t, kafka.brokers)
	defer func() { _ = consumer.Close() }()

	t.Run("producer initialization", func(t *testing.T) {
		ctx := context.Background()
		producer, err := producer.NewAsyncProducer(ctx, serviceName, kafka.brokers,
			producer.WithTelemetry(mockTelemetry),
			producer.WithErrorLogger(mockErrorLogger),
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
		producer, err := producer.NewAsyncProducer(ctx, serviceName, kafka.brokers,
			producer.WithTelemetry(mockTelemetry),
			producer.WithErrorLogger(mockErrorLogger),
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
			Value: sarama.StringEncoder(testMessage + "0"),
		}
		producer.SendMessage(ctx, msg)

		msg = &sarama.ProducerMessage{
			Topic: testTopic,
			Value: sarama.StringEncoder(testMessage + "1"),
		}
		producer.Input() <- msg

		// Verify messages were sent by consuming them.
		partitionConsumer, err := consumer.ConsumePartition(testTopic, testPartition, 0)
		require.NoError(t, err, "Failed to create partition consumer")
		defer func() { _ = partitionConsumer.Close() }()

		for i := range 2 {
			select {
			case msg := <-partitionConsumer.Messages():
				require.Equal(t, testMessage+strconv.Itoa(i), string(msg.Value), "Unexpected message content")
			case <-time.After(5 * time.Second):
				t.Fatal("Timeout waiting for messages")
			}
		}
	})

	t.Run("send multiple messages", func(t *testing.T) {
		ctx := context.Background()
		producer, err := producer.NewAsyncProducer(ctx, serviceName, kafka.brokers,
			producer.WithTelemetry(mockTelemetry),
			producer.WithErrorLogger(mockErrorLogger),
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
		partitionConsumer, err := consumer.ConsumePartition(testTopic, testPartition, 2) // offset 1 since we already consumed message from previous test
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
