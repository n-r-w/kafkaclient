package kafkaproducer

import (
	"context"
	"time"
)

// StubTelemetry is a no-op implementation of ITelemetry interface.
type StubTelemetry struct{}

// CollectWriteLatency does nothing.
func (s *StubTelemetry) CollectWriteLatency(
	ctx context.Context,
	serviceName string,
	duration time.Duration,
	topic string,
	partition int32,
	typeName ProducerType,
	clientID string,
	success bool,
) {
	// No-op implementation
}

// CollectWriteSize does nothing.
func (s *StubTelemetry) CollectWriteSize(
	ctx context.Context,
	serviceName string,
	size int,
	topic string,
	partition int32,
	typeName ProducerType,
	clientID string,
	success bool,
) {
	// No-op implementation
}
