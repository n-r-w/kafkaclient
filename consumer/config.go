package consumer

import (
	"github.com/IBM/sarama"
	kafkaclient "github.com/n-r-w/kafkaclient"
)

const defaultClientID = "kafkaclient"

// DefaultConfig returns the default configuration for ConsumerGroup.
func DefaultConfig() *sarama.Config {
	config := sarama.NewConfig()
	config.ClientID = defaultClientID
	config.Version = kafkaclient.KafkaVersion

	return config
}

// DefaultConfigSASL returns the default configuration for ConsumerGroup with SASL.
func DefaultConfigSASL(
	mechanism sarama.SASLMechanism,
	user string,
	password string,
) *sarama.Config {
	config := DefaultConfig()
	kafkaclient.PrepareDefaultConfigSASL(config, mechanism, user, password)

	return config
}
