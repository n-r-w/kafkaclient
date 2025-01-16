package producer

import (
	"errors"
	"fmt"
	"strings"

	"github.com/cenkalti/backoff/v4"
	kafkaclient "github.com/n-r-w/kafkaclient"

	"github.com/IBM/sarama"
)

// Option option for Producer.
type Option func(*baseProducer)

// WithConfig sets producer configuration.
func WithConfig(config *sarama.Config) Option {
	return func(p *baseProducer) {
		p.config = config
	}
}

// WithName sets producer name.
func WithName(name string) Option {
	return func(p *baseProducer) {
		p.name = name
	}
}

// WithRestartPolicy sets producer restart policy on error.
func WithRestartPolicy(policy backoff.BackOff) Option {
	return func(p *baseProducer) {
		p.restartPolicy = policy
	}
}

// WithTelemetry sets telemetry interface for producer.
func WithTelemetry(telemetry ITelemetry) Option {
	return func(p *baseProducer) {
		p.telemetry = telemetry
	}
}

// WithErrorLogger sets error logger interface for producer.
func WithErrorLogger(errorLogger IErrorLogger) Option {
	return func(p *baseProducer) {
		p.errorLogger = errorLogger
	}
}

type baseProducer struct {
	name          string
	serviceName   string
	restartPolicy backoff.BackOff

	brokers []string
	config  *sarama.Config

	telemetry   ITelemetry
	errorLogger IErrorLogger
}

func newBaseProducer(serviceName string, brokers []string, defaultConfig *sarama.Config, opts ...Option,
) (*baseProducer, error) {
	p := &baseProducer{
		serviceName: serviceName,
		brokers:     brokers,
	}

	p.prepare(defaultConfig, opts)

	if err := p.validate(); err != nil {
		return nil, fmt.Errorf("invalid parameters: %w", err)
	}

	return p, nil
}

func (s *baseProducer) prepare(defaultConfig *sarama.Config, opts []Option) {
	for _, opt := range opts {
		opt(s)
	}

	if s.config == nil {
		s.config = defaultConfig
	}

	if s.config.ClientID == "" {
		clientID := []string{s.serviceName}
		if s.name != "" {
			clientID = append(clientID, s.name)
		}
		s.config.ClientID = strings.Join(clientID, "-")
	}

	if s.telemetry == nil {
		s.telemetry = &StubTelemetry{}
	}
}

func (s *baseProducer) validate() error {
	if len(s.brokers) == 0 {
		return errors.New("brokers is required")
	}
	if s.serviceName == "" {
		return errors.New("service name is required")
	}
	if kafkaclient.ValidateClientID(s.config.ClientID) != nil {
		return fmt.Errorf("invalid client ID: %s", s.config.ClientID)
	}

	return nil
}
