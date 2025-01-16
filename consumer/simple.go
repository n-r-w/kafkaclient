package consumer

import (
	"context"
)

// ConsumerFunc is a function that processes messages.
type ConsumerFunc func(ctx context.Context, topic string, partition int32, msgs []IMessage) error

type simpleProcessor struct {
	consumerName string
	consumerFunc ConsumerFunc
}

func (s *simpleProcessor) ConsumeProcessorName() string {
	return s.consumerName
}

func (s *simpleProcessor) ConsumeKafkaMessages(
	ctx context.Context, topic string, partition int32, msgs []IMessage,
) error {
	return s.consumerFunc(ctx, topic, partition, msgs)
}

func (s *simpleProcessor) ConsumeProcessorStop() {
}

// NewSimple creates a new ConsumerGroup with a single message handler.
func NewSimple(
	ctx context.Context,
	serviceName string,
	brokers []string,
	groupID string,
	consumerName string,
	consumerFunc ConsumerFunc,
	topics []string,
	opts ...Option,
) (*Consumer, error) {
	p := &simpleProcessor{
		consumerName: consumerName,
		consumerFunc: consumerFunc,
	}

	return New(
		ctx,
		serviceName,
		brokers,
		groupID,
		map[IConsumeProcessor][]string{p: topics},
		opts...)
}
