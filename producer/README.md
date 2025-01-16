# Kafka Producer

The package provides two types of Kafka producers:

## Sync Producer

- Blocking message delivery
- Guaranteed message ordering
- Returns delivery status (partition, offset, error)
- Suitable for critical messages requiring confirmation

```go
producer, err := producer.NewSyncProducer(ctx, "example-service", brokers)
if err != nil {
    return nil, fmt.Errorf("failed to create producer: %w", err)
}

msg := &sarama.ProducerMessage{
    Topic: "example-topic",
    Value: sarama.ByteEncoder(jsonData),
}

// Send single message with delivery confirmation
partition, offset, err := producer.SendMessage(ctx, msg)

// Send multiple messages
err = producer.SendMessages(ctx, []*sarama.ProducerMessage{msg1, msg2})
```

## Async Producer

- Non-blocking message delivery
- Higher throughput
- No delivery confirmation
- Suitable for high-volume, non-critical messages

```go
producer, err := producer.NewAsyncProducer(ctx, "example-service", brokers)
if err != nil {
    return nil, fmt.Errorf("failed to create producer: %w", err)
}

// Send single message
producer.SendMessage(ctx, msg)

// Send multiple messages
producer.SendMessages(ctx, []*sarama.ProducerMessage{msg1, msg2})
```

## Producer Configuration

Both producer types support the following configuration options:

```go
// Common options
WithConfig(config *sarama.Config)  // Custom Sarama configuration
WithName(name string)              // Producer name for metrics
WithTelemetry(telemetry ITelemetry) // Custom telemetry implementation
WithErrorLogger(logger IErrorLogger) // Custom error logger

// Sync Producer specific
WithRetryMax(max int)              // Maximum retry attempts
WithRetryBackoff(backoff time.Duration) // Retry backoff duration
WithRequiredAcks(acks sarama.RequiredAcks) // Required acknowledgements
```

## Telemetry Integration

Implement the `ITelemetry` interface to collect producer metrics:

```go
type CustomTelemetry struct{}

func (t *CustomTelemetry) CollectWriteLatency(
    ctx context.Context,
    serviceName string,
    duration time.Duration,
    topic string,
    partition int32,
    typeName ProducerType,
    clientID string,
    success bool,
) {
    // Collect message delivery latency
}

func (t *CustomTelemetry) CollectWriteSize(
    ctx context.Context,
    serviceName string,
    size int,
    topic string,
    partition int32,
    typeName ProducerType,
    clientID string,
    success bool,
) {
    // Collect message size metrics
}
```

## Error Handling

Implement the `IErrorLogger` interface for custom error logging:

```go
type CustomErrorLogger struct{}

func (l *CustomErrorLogger) LogError(ctx context.Context, err error) {
    // Log producer errors with context
    // Includes message delivery failures and connection errors
}
