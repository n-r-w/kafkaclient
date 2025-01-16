package consumer

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	kafkaclient "github.com/n-r-w/kafkaclient"

	"github.com/IBM/sarama"
	"github.com/cenkalti/backoff/v4"
)

// WithConfig sets the configuration for ConsumerGroup.
// Use DefaultConsumerConfig to get the base configuration.
func WithConfig(config *sarama.Config) Option {
	return func(c *Consumer) {
		c.config = config
	}
}

// WithReconnectTimeout sets the timeout for reconnecting to brokers.
func WithReconnectTimeout(timeout time.Duration) Option {
	return func(c *Consumer) {
		c.reconnectTimeout = timeout
	}
}

// WithRestartPolicy sets the restart policy for ConsumerGroup on error.
func WithRestartPolicy(policy backoff.BackOff) Option {
	return func(c *Consumer) {
		c.restartPolicy = policy
	}
}

// WithBatchSize sets the batch size for BatchConsumerType.
func WithBatchSize(batchSize int) Option {
	return func(c *Consumer) {
		c.batchSize = batchSize
	}
}

// WithFlushTimeout sets the message processing timeout for BatchConsumerType.
func WithFlushTimeout(flushTimeout time.Duration) Option {
	return func(c *Consumer) {
		c.flushTimeout = flushTimeout
	}
}

// WithBatchTopics sets topics for which messages should be processed in batches.
func WithBatchTopics(batchTopics ...string) Option {
	return func(c *Consumer) {
		c.batchTopics = batchTopics
	}
}

// WithRetry sets the message processing retry configuration.
func WithRetry(retryConfig backoff.BackOff) Option {
	return func(c *Consumer) {
		c.retryConfig = retryConfig
	}
}

// WithName sets the consumer name.
func WithName(name string) Option {
	return func(c *Consumer) {
		c.name = name
	}
}

// WithTelemetry sets the telemetry implementation for the consumer.
func WithTelemetry(telemetry ITelemetry) Option {
	return func(c *Consumer) {
		c.telemetry = telemetry
	}
}

// WithErrorLogger sets the error logger implementation for the consumer.
func WithErrorLogger(errorLogger IErrorLogger) Option {
	return func(c *Consumer) {
		c.errorLogger = errorLogger
	}
}

// Option represents configuration options for Consumer constructor.
type Option func(*Consumer)

// Consumer implements the ConsumerGroup interface.
type Consumer struct {
	name          string
	serviceName   string
	restartPolicy backoff.BackOff

	batchSize        int
	flushTimeout     time.Duration
	retryConfig      backoff.BackOff
	reconnectTimeout time.Duration

	groupID string
	brokers []string
	topics  []string
	config  *sarama.Config

	batchTopics   []string                       // topics for which messages should be processed in batches
	processorInfo map[string][]IConsumeProcessor // topics and their processors

	cons     *messageHandler
	client   sarama.ConsumerGroup
	wg       *sync.WaitGroup
	cancelFn context.CancelFunc

	telemetry   ITelemetry
	errorLogger IErrorLogger
}

// New creates a new ConsumerGroup instance.
// groupID - consumer group name.
// processors - mapping of handlers to topics.
// If multiple processors are specified for one topic, message processing must be idempotent.
func New(
	_ context.Context,
	serviceName string,
	brokers []string,
	groupID string,
	processors map[IConsumeProcessor][]string,
	opts ...Option,
) (*Consumer, error) {
	const (
		defaultBatchSize    = 1000
		defaultFlushTimeout = time.Millisecond * 100
	)

	c := &Consumer{
		name:        "consumer",
		serviceName: serviceName,
		brokers:     brokers,
		groupID:     groupID,

		wg:               &sync.WaitGroup{},
		processorInfo:    make(map[string][]IConsumeProcessor),
		reconnectTimeout: time.Second,
		config:           DefaultConfig(),
		batchSize:        defaultBatchSize,
		flushTimeout:     defaultFlushTimeout,
	}

	c.prepare(processors, opts)

	if err := c.validate(); err != nil {
		return nil, fmt.Errorf("invalid parameters: %w", err)
	}

	c.cons = newBatchHandler(c, c.batchTopics)

	return c, nil
}

func (c *Consumer) prepare(processors map[IConsumeProcessor][]string, opts []Option) {
	for _, o := range opts {
		o(c)
	}

	if c.config == nil {
		c.config = DefaultConfig()
	}
	if c.config.ClientID == "" {
		clientID := []string{c.serviceName}
		if c.name != "" {
			clientID = append(clientID, c.name)
		}
		c.config.ClientID = strings.Join(clientID, "-")
	}

	for p, ts := range processors {
		for _, t := range ts {
			c.processorInfo[t] = append(c.processorInfo[t], p)
			if !slices.Contains(c.topics, t) {
				c.topics = append(c.topics, t)
			}
		}
	}

	if c.telemetry == nil {
		c.telemetry = &StubTelemetry{}
	}
}

func (c *Consumer) validate() error {
	if len(c.brokers) == 0 {
		return errors.New("brokers is required")
	}
	if len(c.topics) == 0 {
		return errors.New("topics is required")
	}
	if len(c.processorInfo) == 0 {
		return errors.New("processorInfo is required")
	}
	if c.groupID == "" {
		return errors.New("groupID is required")
	}
	if c.serviceName == "" {
		return errors.New("service name is required")
	}

	if err := kafkaclient.ValidateClientID(c.config.ClientID); err != nil {
		return err
	}

	return nil
}

// Start starts the ConsumerGroup.
func (c *Consumer) Start(ctx context.Context) error {
	var err error

	if c.client, err = sarama.NewConsumerGroup(c.brokers, c.groupID, c.config); err != nil {
		return fmt.Errorf("failed to create consumer group: %w", err)
	}

	ctx = context.WithoutCancel(ctx)
	ctx, c.cancelFn = context.WithCancel(ctx)

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for {
			if err = c.client.Consume(ctx, c.topics, c.cons); err != nil {
				if errors.Is(err, sarama.ErrClosedConsumerGroup) {
					return
				}

				if ctx.Err() != nil {
					return
				}

				c.errorLogger.LogError(ctx, fmt.Errorf("failed to consume messages. reconnecting: %w", err))
				time.Sleep(c.reconnectTimeout)
				continue
			}

			if ctx.Err() != nil {
				return
			}

			c.cons.Reset()
		}
	}()
	c.cons.WaitReady()

	return nil
}

// Stop stops the ConsumerGroup.
func (c *Consumer) Stop(ctx context.Context) error {
	if c.client == nil {
		return nil
	}

	c.cancelFn()
	err := c.client.Close()
	c.wg.Wait()

	// stop handlers
	wg := sync.WaitGroup{}
	for _, processors := range c.processorInfo {
		wg.Add(len(processors))
		for _, p := range processors {
			go func(p IConsumeProcessor) {
				defer wg.Done()
				p.ConsumeProcessorStop()
			}(p)
		}
	}
	wg.Wait()

	return err
}
