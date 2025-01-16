package consumer

import (
	"context"
	"time"

	"google.golang.org/protobuf/reflect/protoreflect"
)

//go:generate mockgen -destination=consumer_mock.go -package=consumer -source interface.go

// IMessage represents a consumer message interface.
type IMessage interface {
	Headers() []RecordHeader
	Timestamp() time.Time      // timestamp of the inner message
	BlockTimestamp() time.Time // timestamp of the outer (compressed) block
	Key() []byte
	Value() []byte
	Topic() string
	Partition() int32
	Offset() int64
	MemberID() string
	GenerationID() int32
	Lag() int64
	Context() context.Context
	ReadInProto(protoreflect.ProtoMessage) error
	ReadInJSON(any) error
}

// IConsumeProcessor defines the message handler interface.
// Must be implemented by the structure responsible for message processing.
type IConsumeProcessor interface {
	// ConsumeProcessorName returns the name of the message handler
	ConsumeProcessorName() string
	// ConsumeKafkaMessages processes messages. Commit must be called on success
	ConsumeKafkaMessages(ctx context.Context, topic string, partition int32, msgs []IMessage) error
	// ConsumeProcessorStop is called when the consumer stops
	ConsumeProcessorStop()
}

// ITelemetry interface for working with metrics.
type ITelemetry interface {
	CollectLag(
		ctx context.Context, serviceName string,
		consumerType TextConsumerType, topic string, partition int32,
		clientID, groupID string, lag int64,
	)
	CollectMessageProcessingTime(
		ctx context.Context, serviceName string,
		consumerType TextConsumerType, duration time.Duration,
		topic string, partition int32, clientID, groupID string, success bool,
	)
	CollectMessageProcessingTimeBeforeProcess(
		ctx context.Context, serviceName string,
		consumerType TextConsumerType, duration time.Duration,
		topic string, partition int32, clientID, groupID string, success bool,
	)
	CollectReadSize(
		ctx context.Context, serviceName string,
		consumerType TextConsumerType, size int,
		topic string, partition int32, clientID, groupID string, success bool,
	)
	CollectMessageProcessingRetry(
		ctx context.Context, serviceName string,
		consumerType TextConsumerType, topic string, partition int32, clientID, groupID string,
	)
}

// IErrorLogger interface for error logging.
type IErrorLogger interface {
	LogError(ctx context.Context, err error)
}
