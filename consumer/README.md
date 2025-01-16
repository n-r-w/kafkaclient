# Kafka Consumer

The package provides a Kafka consumer implementation with support for batch processing.

## Consumer Implementation

Implement the `IConsumeProcessor` interface to handle messages:

```go
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
        
        // Add your business logic here:
        // - Save to database
        // - Call external service
        // - Transform data
    }
    return nil
}

func (p *MessageProcessor) ConsumeProcessorStop() {
    // Cleanup resources
}
```

## Consumer Configuration

```go
c, err := kafkaconsumer.New(
    ctx,
    "example-service",
    brokers,
    groupID,
    map[kafkaconsumer.IConsumeProcessor][]string{
        processor: {topic},
    },
    kafkaconsumer.WithBatchTopics(topic),
    kafkaconsumer.WithBatchSize(1000),
    kafkaconsumer.WithFlushTimeout(100*time.Millisecond),
    kafkaconsumer.WithReconnectTimeout(time.Second),
    kafkaconsumer.WithTelemetry(customTelemetry),
    kafkaconsumer.WithErrorLogger(customErrorLogger),
)
```

## Message Processing

The consumer supports two processing modes:

1. **Batch Processing**:
   - Messages are collected into batches
   - Processed when batch size or timeout is reached
   - Configured using `WithBatchTopics()`

2. **Single Message Processing**:
   - Messages are processed individually
   - Default mode for non-batch topics

## Configuration Options

### Consumer Options

- `WithBatchSize(size int)`: Set batch size for processing
- `WithFlushTimeout(timeout time.Duration)`: Set batch flush timeout
- `WithBatchTopics(topics ...string)`: Configure topics for batch processing
- `WithReconnectTimeout(timeout time.Duration)`: Set broker reconnect timeout
- `WithTelemetry(telemetry ITelemetry)`: Set custom telemetry implementation
- `WithErrorLogger(logger IErrorLogger)`: Set custom error logger
- `WithRetry(config backoff.BackOff)`: Configure message processing retry policy
- `WithConfig(config *sarama.Config)`: Set custom Sarama configuration
- `WithName(name string)`: Set consumer name for metrics

## Telemetry Integration

Implement the `ITelemetry` interface to collect metrics:

```go
type CustomTelemetry struct{}

func (t *CustomTelemetry) CollectLag(
    ctx context.Context, serviceName string,
    consumerType TextConsumerType, topic string, partition int32,
    clientID, groupID string, lag int64,
) {
    // Collect consumer lag metrics
}

func (t *CustomTelemetry) CollectMessageProcessingTime(
    ctx context.Context, serviceName string,
    consumerType TextConsumerType, duration time.Duration,
    topic string, partition int32, clientID, groupID string, success bool,
) {
    // Collect processing time metrics
}

func (t *CustomTelemetry) CollectMessageProcessingTimeBeforeProcess(
    ctx context.Context, serviceName string,
    consumerType TextConsumerType, duration time.Duration,
    topic string, partition int32, clientID, groupID string, success bool,
) {
    // Collect time before processing metrics
}

func (t *CustomTelemetry) CollectReadSize(
    ctx context.Context, serviceName string,
    consumerType TextConsumerType, size int,
    topic string, partition int32, clientID, groupID string, success bool,
) {
    // Collect message size metrics
}

func (t *CustomTelemetry) CollectMessageProcessingRetry(
    ctx context.Context, serviceName string,
    consumerType TextConsumerType, topic string, partition int32, clientID, groupID string,
) {
    // Collect retry metrics
}
```

## Error Handling

Implement the `IErrorLogger` interface for custom error logging:

```go
type CustomErrorLogger struct{}

func (l *CustomErrorLogger) LogError(ctx context.Context, err error) {
    // Log errors with context
}
