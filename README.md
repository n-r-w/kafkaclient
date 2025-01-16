# Kafka Client

A Go package providing Kafka consumer and producer implementations with support for batch processing and async/sync message delivery.

## Features

- **Sarama Integration**: Built on top of [github.com/IBM/sarama](https://github.com/IBM/sarama) for reliable Kafka client implementation
- **Batch Consumer**: Process messages in batches with configurable size and timeout
- **Sync Producer**: Blocking message delivery with guaranteed ordering
- **Async Producer**: Non-blocking message delivery for high throughput
- **Structured Logging**: Built-in telemetry and metrics
- **Graceful Shutdown**: Proper resource cleanup on termination
- **Error Recovery**: Automatic retry and panic recovery mechanisms
- **Telemetry Integration**: Built-in metrics collection for monitoring

## Installation

```bash
go get github.com/n-r-w/kafkaclient
```

## Documentation

- [Consumer Documentation](consumer/README.md)
- [Producer Documentation](producer/README.md)

## Example

[full example](example/main.go)
