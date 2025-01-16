package kafkaconsumer

import (
	"context"
	"time"
)

// StubTelemetry is a no-op implementation of ITelemetry interface.
type StubTelemetry struct{}

func (s *StubTelemetry) CollectLag(
	ctx context.Context, serviceName string, consumerType TextConsumerType,
	topic string, partition int32, clientID string, groupID string, lag int64,
) {
	// No-op implementation
}

func (s *StubTelemetry) CollectMessageProcessingTime(
	ctx context.Context, serviceName string, consumerType TextConsumerType,
	duration time.Duration, topic string, partition int32, clientID string, groupID string, success bool,
) {
	// No-op implementation
}

func (s *StubTelemetry) CollectMessageProcessingTimeBeforeProcess(
	ctx context.Context, serviceName string, consumerType TextConsumerType,
	duration time.Duration, topic string, partition int32, clientID string, groupID string, success bool,
) {
	// No-op implementation
}

func (s *StubTelemetry) CollectReadSize(
	ctx context.Context, serviceName string,
	consumerType TextConsumerType, size int, topic string, partition int32, clientID string, groupID string, success bool,
) {
	// No-op implementation
}

func (s *StubTelemetry) CollectMessageProcessingRetry(
	ctx context.Context, serviceName string,
	consumerType TextConsumerType, topic string, partition int32, clientID string, groupID string,
) {
	// No-op implementation
}
