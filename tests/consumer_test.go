package realtest

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/IBM/sarama"
	kafkaconsumer "github.com/n-r-w/kafkaclient/consumer"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

// produceTestMessage produces a test message to the specified topic.
func (k *testKafkaContainer) produceTestMessage(t *testing.T, topic, message string) {
	config := sarama.NewConfig()
	config.Version = sarama.V3_5_0_0
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(k.brokers, config)
	require.NoError(t, err, "Failed to create producer")
	defer func() { _ = producer.Close() }()

	_, _, err = producer.SendMessage(&sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	})
	require.NoError(t, err, "Failed to send message")
}

// TestConsumer_Integration tests the Consumer with a real Kafka instance.
func TestConsumer_Integration(t *testing.T) {
	const (
		testTopic     = "test-topic"
		testMessage   = "test-message"
		serviceName   = "test-service"
		testPartition = int32(0)
		groupID       = "test-group"
	)

	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Setup mock controller
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Create mock implementations
	mockTelemetry := kafkaconsumer.NewMockITelemetry(ctrl)
	mockErrorLogger := kafkaconsumer.NewMockIErrorLogger(ctrl)

	// Setup expectations for telemetry and error logging
	mockTelemetry.EXPECT().
		CollectMessageProcessingTime(gomock.Any(), serviceName, gomock.Any(), gomock.Any(), testTopic, gomock.Any(), gomock.Any(), gomock.Any(), true).
		AnyTimes()
	mockTelemetry.EXPECT().
		CollectReadSize(gomock.Any(), serviceName, gomock.Any(), gomock.Any(), testTopic, gomock.Any(), gomock.Any(), gomock.Any(), true).
		AnyTimes()
	mockTelemetry.EXPECT().
		CollectLag(gomock.Any(), serviceName, gomock.Any(), testTopic, testPartition, gomock.Any(), groupID, gomock.Any()).
		AnyTimes()
	mockTelemetry.EXPECT().
		CollectMessageProcessingTimeBeforeProcess(gomock.Any(), serviceName, gomock.Any(), gomock.Any(), testTopic, gomock.Any(), gomock.Any(), gomock.Any(), true).
		AnyTimes()
	mockErrorLogger.EXPECT().
		LogError(gomock.Any(), gomock.Any()).
		AnyTimes()

	// Setup Kafka container
	kafka := setupKafka(t)
	defer kafka.cleanup(t)

	// Create test topic
	kafka.createTopic(t, testTopic, 1)

	t.Run("consumer initialization", func(t *testing.T) {
		// Create a mock processor
		mockProcessor := kafkaconsumer.NewMockIConsumeProcessor(ctrl)
		mockProcessor.EXPECT().
			ConsumeProcessorName().
			Return("test-processor").
			AnyTimes()
		mockProcessor.EXPECT().
			ConsumeProcessorStop().
			AnyTimes()

		// Create consumer
		consumer, err := kafkaconsumer.New(
			context.Background(),
			serviceName,
			kafka.brokers,
			groupID,
			map[kafkaconsumer.IConsumeProcessor][]string{
				mockProcessor: {testTopic},
			},
			kafkaconsumer.WithTelemetry(mockTelemetry),
			kafkaconsumer.WithErrorLogger(mockErrorLogger),
		)
		require.NoError(t, err)
		require.NotNil(t, consumer)

		// Start consumer
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		err = consumer.Start(ctx)
		require.NoError(t, err)

		// Stop consumer
		err = consumer.Stop(ctx)
		require.NoError(t, err)
	})

	t.Run("consume single message", func(t *testing.T) {
		// Create message channel to verify consumption
		messageCh := make(chan kafkaconsumer.IMessage, 1)

		// Create a mock processor that will send received message to the channel
		mockProcessor := kafkaconsumer.NewMockIConsumeProcessor(ctrl)
		mockTelemetry.EXPECT().
			CollectLag(gomock.Any(), serviceName, gomock.Any(), testTopic, testPartition, gomock.Any(), groupID, gomock.Any()).
			AnyTimes()
		mockProcessor.EXPECT().
			ConsumeProcessorName().
			Return("test-processor").
			AnyTimes()
		mockProcessor.EXPECT().
			ConsumeProcessorStop().
			AnyTimes()
		mockProcessor.EXPECT().
			ConsumeKafkaMessages(gomock.Any(), testTopic, gomock.Any(), gomock.Any()).
			DoAndReturn(func(_ context.Context, _ string, _ int32, msgs []kafkaconsumer.IMessage) error {
				for _, msg := range msgs {
					messageCh <- msg
				}
				return nil
			}).
			AnyTimes()

		// Create and start consumer
		consumer, err := kafkaconsumer.New(
			context.Background(),
			serviceName,
			kafka.brokers,
			groupID,
			map[kafkaconsumer.IConsumeProcessor][]string{
				mockProcessor: {testTopic},
			},
			kafkaconsumer.WithTelemetry(mockTelemetry),
			kafkaconsumer.WithErrorLogger(mockErrorLogger),
		)
		require.NoError(t, err)
		require.NotNil(t, consumer)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		err = consumer.Start(ctx)
		require.NoError(t, err)

		// Produce test message
		kafka.produceTestMessage(t, testTopic, testMessage)

		// Wait for message to be consumed
		select {
		case msg := <-messageCh:
			require.Equal(t, testMessage, string(msg.Value()))
			require.Equal(t, testTopic, msg.Topic())
		case <-ctx.Done():
			t.Fatal("Timeout waiting for message")
		}

		// Stop consumer
		err = consumer.Stop(ctx)
		require.NoError(t, err)
	})

	t.Run("consume batch messages", func(t *testing.T) {
		const (
			testBatchSize    = 9
			testMessageCount = 500
		)

		// Create message channel to verify consumption
		// guarantee that the channel will immediately get the entire batch
		expectedBatches := (testMessageCount + testBatchSize - 1) / testBatchSize // ceil division
		messagesCh := make(chan []kafkaconsumer.IMessage, expectedBatches)

		// Create a mock processor that will send received messages to the channel
		mockProcessor := kafkaconsumer.NewMockIConsumeProcessor(ctrl)
		mockTelemetry.EXPECT().
			CollectLag(gomock.Any(), serviceName, gomock.Any(), testTopic, testPartition, gomock.Any(), groupID, gomock.Any()).
			AnyTimes()
		mockProcessor.EXPECT().
			ConsumeProcessorName().
			Return("test-processor").
			AnyTimes()
		mockProcessor.EXPECT().
			ConsumeProcessorStop().
			AnyTimes()
		mockProcessor.EXPECT().
			ConsumeKafkaMessages(gomock.Any(), testTopic, gomock.Any(), gomock.Any()).
			DoAndReturn(func(_ context.Context, _ string, _ int32, msgs []kafkaconsumer.IMessage) error {
				messagesCh <- msgs
				return nil
			}).
			MinTimes(expectedBatches).MaxTimes(expectedBatches)

		// Create and start consumer with batch configuration
		consumer, err := kafkaconsumer.New(
			context.Background(),
			serviceName,
			kafka.brokers,
			groupID,
			map[kafkaconsumer.IConsumeProcessor][]string{
				mockProcessor: {testTopic},
			},
			kafkaconsumer.WithTelemetry(mockTelemetry),
			kafkaconsumer.WithErrorLogger(mockErrorLogger),
			kafkaconsumer.WithBatchSize(testBatchSize),
			kafkaconsumer.WithFlushTimeout(100*time.Millisecond),
			kafkaconsumer.WithBatchTopics(testTopic),
		)
		require.NoError(t, err)
		require.NotNil(t, consumer)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		err = consumer.Start(ctx)
		require.NoError(t, err)

		// Produce test messages
		for i := 1; i <= testMessageCount; i++ {
			kafka.produceTestMessage(t, testTopic, fmt.Sprintf("%s%d", testMessage, i))
		}

		// Process all batches
		processedMessages := 0
		for i := 0; i < expectedBatches; i++ {
			select {
			case msgs := <-messagesCh:
				// Calculate expected batch size
				remainingMessages := testMessageCount - processedMessages
				expectedSize := testBatchSize
				if remainingMessages < testBatchSize {
					expectedSize = remainingMessages
				}

				require.Len(t, msgs, expectedSize)

				// Verify each message in the batch
				for j := 0; j < expectedSize; j++ {
					messageNum := processedMessages + j + 1
					require.Equal(t, fmt.Sprintf("%s%d", testMessage, messageNum), string(msgs[j].Value()))
					require.Equal(t, testTopic, msgs[j].Topic())
				}

				processedMessages += expectedSize
			case <-ctx.Done():
				t.Fatalf("Timeout waiting for batch %d", i+1)
			}
		}

		// Verify total processed messages
		require.Equal(t, testMessageCount, processedMessages)

		// Stop consumer
		err = consumer.Stop(ctx)
		require.NoError(t, err)
	})
}
