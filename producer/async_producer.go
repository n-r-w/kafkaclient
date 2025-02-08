package producer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

const asyncTimeHeader = "async-produce-start-time"

// AsyncProducer asynchronous producer.
type AsyncProducer struct {
	*baseProducer

	producer sarama.AsyncProducer
	input    chan *sarama.ProducerMessage
	wg       sync.WaitGroup
}

// NewAsyncProducer creates a new instance of asynchronous producer.
func NewAsyncProducer(_ context.Context, serviceName string, brokers []string,
	opts ...Option,
) (*AsyncProducer, error) {
	base, err := newBaseProducer(serviceName, brokers, DefaultAsyncConfig(), opts...)
	if err != nil {
		return nil, err
	}

	p := &AsyncProducer{
		baseProducer: base,
		input:        make(chan *sarama.ProducerMessage),
	}

	return p, nil
}

// SendMessage sends a message to the queue.
func (a *AsyncProducer) SendMessage(ctx context.Context, msg *sarama.ProducerMessage) {
	msg.Headers = injectAsyncProducerTimeHeader(time.Now(), msg.Headers)
	a.producer.Input() <- msg
}

// Input returns the input channel.
func (a *AsyncProducer) Input() chan<- *sarama.ProducerMessage {
	return a.input
}

// SendMessages sends messages to the queue.
func (a *AsyncProducer) SendMessages(ctx context.Context, msgs []*sarama.ProducerMessage) {
	for _, msg := range msgs {
		a.SendMessage(ctx, msg) // sending in parallel doesn't make sense because SendMessage uses a channel internally
	}
}

// Start starts the producer.
func (a *AsyncProducer) Start(ctx context.Context) error {
	// required for metrics to work
	a.config.Producer.Return.Successes = true
	a.config.Producer.Return.Errors = true

	ctx = context.WithoutCancel(ctx)

	var err error
	a.producer, err = sarama.NewAsyncProducer(a.brokers, a.config)
	if err != nil {
		return fmt.Errorf("failed to create async producer: %w", err)
	}

	a.wg.Add(3) //nolint:mnd // ok

	go func() {
		defer a.wg.Done()

		for err := range a.producer.Errors() {
			a.errorLogger.LogError(ctx, fmt.Errorf("failed to send message: %w", err))

			a.telemetry.CollectWriteSize(
				ctx, a.serviceName,
				err.Msg.Value.Length(),
				err.Msg.Topic,
				err.Msg.Partition,
				AsyncProducerType,
				a.config.ClientID,
				false)

			t, errExtr := extractAsyncProducerTimeHeader(err.Msg.Headers)
			if errExtr != nil {
				a.errorLogger.LogError(ctx, fmt.Errorf("failed to extract async producer time header: %w", err))
				continue
			}
			a.telemetry.CollectWriteLatency(
				ctx, a.serviceName,
				time.Since(t),
				err.Msg.Topic,
				err.Msg.Partition,
				AsyncProducerType,
				a.config.ClientID,
				false)
		}
	}()

	go func() {
		defer a.wg.Done()

		for msg := range a.producer.Successes() {
			t, errExtr := extractAsyncProducerTimeHeader(msg.Headers)
			if errExtr != nil {
				a.errorLogger.LogError(ctx, fmt.Errorf("failed to extract async producer time header: %w", errExtr))
				continue
			}
			a.telemetry.CollectWriteLatency(
				ctx, a.serviceName,
				time.Since(t),
				msg.Topic,
				msg.Partition,
				AsyncProducerType,
				a.config.ClientID,
				true)
		}
	}()

	go func() {
		defer a.wg.Done()

		for msg := range a.input {
			msg.Headers = injectAsyncProducerTimeHeader(time.Now(), msg.Headers)
			a.producer.Input() <- msg
		}
	}()

	return nil
}

// Stop stops the producer.
func (a *AsyncProducer) Stop(_ context.Context) error {
	if a.producer == nil {
		return nil
	}

	close(a.input)

	err := a.producer.Close()
	a.wg.Wait()

	return err
}

// injectAsyncProducerTimeHeader adds the send time to the message header.
func injectAsyncProducerTimeHeader(t time.Time, headers []sarama.RecordHeader) []sarama.RecordHeader {
	start, _ := t.MarshalJSON()
	return append(headers, sarama.RecordHeader{
		Key:   []byte(asyncTimeHeader),
		Value: start,
	})
}

// extractAsyncProducerTimeHeader extracts the send time from the message header.
func extractAsyncProducerTimeHeader(headers []sarama.RecordHeader) (time.Time, error) {
	var t time.Time
	var err error

	for _, h := range headers {
		s := string(h.Key)
		if s == asyncTimeHeader {
			err = t.UnmarshalJSON(h.Value)
			if err != nil {
				return t, err
			}
		}
	}

	if t.IsZero() {
		return t, fmt.Errorf("%s header not found", asyncTimeHeader)
	}

	return t, nil
}
