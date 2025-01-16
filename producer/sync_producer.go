package kafkaproducer

import (
	"context"
	"fmt"
	"time"

	"github.com/IBM/sarama"
)

// SyncProducer synchronous producer.
type SyncProducer struct {
	*baseProducer

	producer sarama.SyncProducer
}

// NewSyncProducer creates a new instance of synchronous producer.
func NewSyncProducer(_ context.Context, serviceName string, brokers []string, opts ...Option,
) (*SyncProducer, error) {
	base, err := newBaseProducer(serviceName, brokers, DefaultSyncConfig(), opts...)
	if err != nil {
		return nil, err
	}

	p := &SyncProducer{
		baseProducer: base,
	}

	p.config.Producer.Return.Successes = true // required for synchronous producer

	return p, nil
}

// SendMessage synchronously sends a message to the queue.
func (s *SyncProducer) SendMessage(ctx context.Context,
	msg *sarama.ProducerMessage,
) (partition int32, offset int64, err error) {
	now := time.Now()
	defer func() {
		s.telemetry.CollectWriteLatency(
			ctx, s.serviceName,
			time.Since(now),
			msg.Topic,
			partition,
			SyncProducerType,
			s.config.ClientID,
			err == nil)
		s.telemetry.CollectWriteSize(
			ctx, s.serviceName,
			msg.Value.Length(),
			msg.Topic,
			partition,
			SyncProducerType,
			s.config.ClientID,
			err == nil)

		if err != nil {
			s.errorLogger.LogError(ctx, fmt.Errorf("failed to send message: %w", err))
		}
	}()

	return s.producer.SendMessage(msg)
}

// SendMessages synchronously sends messages to the queue.
func (s *SyncProducer) SendMessages(ctx context.Context, msgs []*sarama.ProducerMessage) (err error) {
	if len(msgs) == 0 {
		return nil
	}

	now := time.Now()
	defer func() {
		duration := time.Since(now)
		for _, msg := range msgs {
			s.telemetry.CollectWriteLatency(
				ctx, s.serviceName,
				duration,
				msg.Topic,
				msg.Partition,
				SyncProducerType,
				s.config.ClientID,
				err == nil)
			s.telemetry.CollectWriteSize(
				ctx, s.serviceName,
				msg.Value.Length(),
				msg.Topic,
				msg.Partition,
				SyncProducerType,
				s.config.ClientID,
				err == nil)
		}

		if err != nil {
			s.errorLogger.LogError(ctx, fmt.Errorf("failed to send messages: %w", err))
		}
	}()

	return s.producer.SendMessages(msgs)
}

// Start starts the producer.
func (s *SyncProducer) Start(_ context.Context) error {
	var err error
	s.producer, err = sarama.NewSyncProducer(s.brokers, s.config)
	if err != nil {
		return fmt.Errorf("failed to create sync producer: %w", err)
	}

	return nil
}

// Stop stops the producer.
func (s *SyncProducer) Stop(_ context.Context) error {
	if s.producer == nil {
		return nil
	}

	return s.producer.Close()
}
