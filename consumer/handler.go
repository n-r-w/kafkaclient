package consumer

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/cenkalti/backoff/v4"
	"golang.org/x/sync/errgroup"
)

type TextConsumerType string

const (
	BatchConsumerType  TextConsumerType = "batch"
	SingleConsumerType TextConsumerType = "single"
)

// messageKey represents a key for messages in batchProcessor.
type messageKey struct {
	topic     string
	partition int32
}

var _ sarama.ConsumerGroupHandler = (*messageHandler)(nil)

// messageHandler implements the message handler for ConsumerGroup.
type messageHandler struct {
	group *Consumer
	ready chan bool

	batchTopics map[string]struct{} // topics that require batch message processing

	processors map[messageKey]*batchProcessor
	mu         sync.Mutex
}

func newBatchHandler(group *Consumer, batchTopics []string) *messageHandler {
	h := &messageHandler{
		group:       group,
		ready:       make(chan bool),
		processors:  make(map[messageKey]*batchProcessor),
		batchTopics: make(map[string]struct{}, len(batchTopics)),
	}

	for _, topic := range batchTopics {
		h.batchTopics[topic] = struct{}{}
	}

	return h
}

// ConsumeClaim starts message processing from the queue.
func (c *messageHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	ctx := session.Context()

	// on exit clean up batchProcessor and their buffers
	// no need to process messages in buffers since Kafka will resend them if not committed
	defer func() {
		c.mu.Lock()
		defer c.mu.Unlock()

		wg := sync.WaitGroup{}
		wg.Add(len(c.processors))

		for _, processor := range c.processors {
			go func(p *batchProcessor) {
				defer wg.Done()
				p.stop()
			}(processor)
		}
		wg.Wait()

		c.processors = make(map[messageKey]*batchProcessor, len(c.processors))
	}()

	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				return nil
			}

			lag := claim.HighWaterMarkOffset() - message.Offset
			c.group.telemetry.CollectLag(
				ctx, c.group.serviceName,
				BatchConsumerType, message.Topic, message.Partition,
				c.group.config.ClientID, c.group.groupID, lag)

			if _, ok = c.batchTopics[message.Topic]; !ok {
				// no batching needed for this topic
				if err := c.processSingleMessage(ctx,
					&Message{
						session: session,
						message: message,
						lag:     lag,
					}); err != nil {
					return err
				}
				continue
			}

			key := messageKey{
				topic:     message.Topic,
				partition: message.Partition,
			}

			c.mu.Lock()
			processor, ok := c.processors[key]
			if !ok {
				processor = newBatchProcessor(ctx, c, message.Topic, message.Partition)
				c.processors[key] = processor
			}
			c.mu.Unlock()

			if err := processor.addMessage(ctx, session, message, lag); err != nil {
				return err
			}

		case <-ctx.Done():
			return nil
		}
	}
}

func (c *messageHandler) processSingleMessage(ctx context.Context, message *Message) (err error) {
	startTime := time.Now()

	defer func() {
		c.group.telemetry.CollectMessageProcessingTime(
			ctx, c.group.serviceName,
			SingleConsumerType,
			time.Since(startTime),
			message.Topic(),
			message.Partition(),
			c.group.config.ClientID,
			c.group.groupID,
			err == nil)

		c.group.telemetry.CollectMessageProcessingTimeBeforeProcess(
			ctx, c.group.serviceName,
			SingleConsumerType,
			startTime.Sub(message.Timestamp()),
			message.Topic(),
			message.Partition(),
			c.group.config.ClientID,
			c.group.groupID,
			err == nil)

		c.group.telemetry.CollectReadSize(
			ctx, c.group.serviceName,
			SingleConsumerType,
			len(message.Value()),
			message.Topic(),
			message.Partition(),
			c.group.config.ClientID,
			c.group.groupID,
			err == nil)

		if err != nil {
			c.group.errorLogger.LogError(ctx, fmt.Errorf("failed to process message: %w", err))
		}
	}()

	if processors, ok := c.group.processorInfo[message.Payload().Topic]; ok {
		err = c.consumeSingleProcessors(ctx, processors, message)
	} else {
		err = fmt.Errorf("no processor for topic %s", message.Payload().Topic)
	}
	if err != nil {
		return err
	}

	message.session.MarkMessage(message.Payload(), "")

	return nil
}

func (c *messageHandler) consumeSingleProcessors(
	ctx context.Context,
	processors []IConsumeProcessor,
	message IMessage,
) error {
	messages := []IMessage{message}

	// if any processor fails, we must return an error
	// so there's no point in further message processing
	wg, ctxWg := errgroup.WithContext(ctx)
	for _, p := range processors {
		pCopy := p
		wg.Go(func() error {
			if err := c.recovery(c.retry(pCopy.ConsumeKafkaMessages, pCopy.ConsumeProcessorName(), SingleConsumerType))(
				ctxWg, message.Topic(), message.Partition(), messages); err != nil {
				return fmt.Errorf("processor %s failed: %w", pCopy.ConsumeProcessorName(), err)
			}
			return nil
		})
	}

	return wg.Wait()
}

// Setup performs handler initialization.
func (c *messageHandler) Setup(sarama.ConsumerGroupSession) error {
	close(c.ready)
	return nil
}

// Cleanup performs resource cleanup.
func (c *messageHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// WaitReady waits until the ConsumerGroup is started.
func (c *messageHandler) WaitReady() {
	<-c.ready
}

// Reset resets the ConsumerGroup state after an error.
func (c *messageHandler) Reset() {
	c.ready = make(chan bool)
}

func (c *messageHandler) retry(
	handler func(ctx context.Context, topic string, partition int32, msgs []IMessage) error,
	_ string, consumerType TextConsumerType,
) func(ctx context.Context, topic string, partition int32, msgs []IMessage) error {
	if c.group.retryConfig == nil {
		return handler
	}

	return func(ctx context.Context, topic string, partition int32, msgs []IMessage) error {
		if len(msgs) == 0 {
			return nil
		}

		return backoff.RetryNotify(
			func() error {
				return handler(ctx, topic, partition, msgs)
			},
			backoff.WithContext(c.group.retryConfig, ctx),
			func(err error, delay time.Duration) {
				// a single batch cannot contain messages from different topics and partitions
				if err != nil {
					c.group.errorLogger.LogError(ctx, fmt.Errorf("failed to process messages, retrying: %w", err))
				}

				c.group.telemetry.CollectMessageProcessingRetry(
					ctx, c.group.serviceName,
					consumerType, topic, partition, c.group.config.ClientID, c.group.groupID)
			},
		)
	}
}

func (c *messageHandler) recovery(
	handler func(ctx context.Context, topic string, partition int32, msgs []IMessage) error,
) func(ctx context.Context, topic string, partition int32, msgs []IMessage) error {
	return func(ctx context.Context, topic string, partition int32, msgs []IMessage) (err error) {
		if len(msgs) == 0 {
			return nil
		}

		defer func() {
			if r := recover(); r != nil {
				c.group.errorLogger.LogError(ctx,
					fmt.Errorf("panic, failed to process messages: %v, stack trace: %s, topic: %s, partition: %d, offset: %d",
						r, debug.Stack(), topic, partition, msgs[0].Offset()))

				err = fmt.Errorf("panic: failed to process messages: %v", r)
			}
		}()

		return handler(ctx, topic, partition, msgs)
	}
}
