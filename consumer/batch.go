package consumer

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"golang.org/x/sync/errgroup"
)

// Sarama processes unique topic/partition combinations in a single goroutine.
// So create a separate handler for each topic/partition pair.
type batchProcessor struct {
	handler *messageHandler

	topic     string
	partition int32

	buffer []IMessage
	mu     sync.Mutex

	wg          sync.WaitGroup
	cancelFn    context.CancelFunc
	flushTicker *time.Ticker
}

func newBatchProcessor(ctx context.Context, handler *messageHandler, topic string, partition int32) *batchProcessor {
	b := &batchProcessor{
		handler:     handler,
		topic:       topic,
		partition:   partition,
		buffer:      make([]IMessage, 0, handler.group.batchSize),
		flushTicker: time.NewTicker(handler.group.flushTimeout),
	}

	b.flushWorker(ctx)

	return b
}

func (c *batchProcessor) addMessage(ctx context.Context, session sarama.ConsumerGroupSession,
	message *sarama.ConsumerMessage, lag int64,
) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	msg := &Message{
		session: session,
		message: message,
		lag:     lag,
	}

	c.buffer = append(c.buffer, msg)

	if len(c.buffer) >= c.handler.group.batchSize {
		err := c.flush(ctx)
		return err
	}

	return nil
}

func (c *batchProcessor) stop() {
	c.cancelFn()
	c.flushTicker.Stop()
	c.wg.Wait()
}

func (c *batchProcessor) flushWorker(ctx context.Context) {
	c.wg.Add(1)
	ctx, c.cancelFn = context.WithCancel(ctx)

	go func() {
		defer c.wg.Done()

		for {
			select {
			case <-ctx.Done():
				return
			case <-c.flushTicker.C:
				// if we're already inside flush, there's no point waiting here,
				// because the ticker will be reset at the end of flush
				if c.mu.TryLock() {
					if err := c.flush(ctx); err != nil {
						c.handler.group.errorLogger.LogError(ctx, err)
					}

					c.mu.Unlock()
				}
			}
		}
	}()
}

func (c *batchProcessor) flush(ctx context.Context) (err error) {
	defer c.flushTicker.Reset(c.handler.group.flushTimeout)

	if len(c.buffer) == 0 {
		return nil
	}

	startTime := time.Now()

	defer func() {
		duration := time.Since(startTime)
		for i, msg := range c.buffer {
			msgValue, _ := msg.(*Message)

			c.handler.group.telemetry.CollectMessageProcessingTime(
				ctx,
				c.handler.group.serviceName,
				BatchConsumerType,
				duration,
				c.topic,
				c.partition,
				c.handler.group.config.ClientID,
				c.handler.group.groupID,
				err == nil)

			c.handler.group.telemetry.CollectMessageProcessingTimeBeforeProcess(
				ctx,
				c.handler.group.serviceName,
				BatchConsumerType,
				// there will be some inaccuracy here, as the time should be measured
				// when the message is added to the buffer, but it can be neglected
				startTime.Sub(msgValue.Timestamp()),
				c.topic,
				c.partition,
				c.handler.group.config.ClientID,
				c.handler.group.groupID,
				err == nil)

			c.handler.group.telemetry.CollectReadSize(
				ctx,
				c.handler.group.serviceName,
				BatchConsumerType,
				len(msgValue.Value()),
				c.topic,
				c.partition,
				c.handler.group.config.ClientID,
				c.handler.group.groupID,
				err == nil)

			c.buffer[i] = nil // to avoid memory allocation after clearing the buffer with c.buffer[:0]
		}

		c.buffer = c.buffer[:0] // clear the buffer
	}()

	if processors, ok := c.handler.group.processorInfo[c.topic]; ok {
		if err = c.consumeProcessors(ctx, c.topic, c.partition, processors, c.buffer); err != nil {
			return err
		}
	} else {
		return fmt.Errorf("no processor for topic %s", c.topic)
	}

	// commit the entire batch
	for _, iMsg := range c.buffer {
		msg, _ := iMsg.(*Message)
		msg.session.MarkMessage(msg.message, "")
	}

	return nil
}

func (c *batchProcessor) consumeProcessors(ctx context.Context,
	topic string,
	partition int32,
	processors []IConsumeProcessor,
	messages []IMessage,
) error {
	// copy messages slice to avoid race conditions, because the slice items will be
	// modified later in flush function
	messagesCopy := make([]IMessage, len(messages))
	copy(messagesCopy, messages)

	// if at least one processor fails, the entire batch fails, so there is no point in
	// continuing to process the message.
	eg, egCtx := errgroup.WithContext(ctx)
	for _, p := range processors {
		pCopy := p
		eg.Go(func() error {
			if err := c.handler.recovery(c.handler.retry(
				pCopy.ConsumeKafkaMessages, pCopy.ConsumeProcessorName(), BatchConsumerType))(
				egCtx, topic, partition, messagesCopy); err != nil {
				if errors.Is(err, context.Canceled) {
					return nil
				}
				return fmt.Errorf("processor %s failed: %w", pCopy.ConsumeProcessorName(), err)
			}

			return nil
		})
	}

	return eg.Wait()
}
