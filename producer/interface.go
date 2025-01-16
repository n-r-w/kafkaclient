package kafkaproducer

import (
	"context"
	"time"

	"github.com/IBM/sarama"
)

//go:generate mockgen -destination=sarama_mocks.go -package=kafkaproducer github.com/IBM/sarama AsyncProducer,SyncProducer
//go:generate mockgen -destination=producer_mock.go -package=kafkaproducer -source interface.go

// IAsyncProducer asynchronous producer.
type IAsyncProducer interface {
	// SendMessage sends message to queue.
	// Producer doesn't support context operations (using WithTimeout doesn't make sense).
	// Context is here just in case (e.g. for metrics).
	// When sending a message, the start time is added to its header,
	// so resending the same message is not allowed.
	SendMessage(ctx context.Context, msg *sarama.ProducerMessage)

	// SendMessages sends messages to queue.
	// If context is canceled, messages won't be sent and false will be returned.
	// Producer doesn't support context operations (using WithTimeout doesn't make sense).
	// Context is here just in case (e.g. for metrics).
	// When sending messages, the start time is added to their headers,
	// so resending the same messages is not allowed.
	SendMessages(ctx context.Context, msgs []*sarama.ProducerMessage)
}

// ISyncProducer synchronous producer.
type ISyncProducer interface {
	// SendMessage synchronously sends message to queue.
	// Producer doesn't support context operations (using WithTimeout doesn't make sense).
	// Context is here for working with metrics.
	SendMessage(ctx context.Context, msg *sarama.ProducerMessage) (partition int32, offset int64, err error)

	// SendMessages synchronously sends messages to queue.
	// Synchronous producer doesn't support context operations (using WithTimeout doesn't make sense).
	// Context is here for working with metrics.
	SendMessages(ctx context.Context, msgs []*sarama.ProducerMessage) error
}

type ProducerType string

const (
	SyncProducerType  ProducerType = "sync-producer"
	AsyncProducerType ProducerType = "async-producer"
)

// ITelemetry interface for working with metrics.
type ITelemetry interface {
	CollectWriteLatency(
		ctx context.Context,
		serviceName string,
		duration time.Duration,
		topic string,
		partition int32,
		typeName ProducerType,
		clientID string,
		success bool,
	)
	CollectWriteSize(
		ctx context.Context,
		serviceName string,
		size int,
		topic string,
		partition int32,
		typeName ProducerType,
		clientID string,
		success bool,
	)
}

// IErrorLogger interface for error logging.
type IErrorLogger interface {
	LogError(ctx context.Context, err error)
}
