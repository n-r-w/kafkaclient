package producer

import (
	"github.com/IBM/sarama"
	kafkaclient "github.com/n-r-w/kafkaclient"
)

const defaultClientID = "kafkaclient"

// DefaultAsyncConfig returns the default configuration for AsyncProducer.
func DefaultAsyncConfig() *sarama.Config {
	config := sarama.NewConfig()
	config.ClientID = defaultClientID

	config.Version = kafkaclient.KafkaVersion
	config.Producer.RequiredAcks = sarama.WaitForAll

	return config
}

// DefaultAsyncConfigSASL returns the default configuration for AsyncProducer with SASL.
func DefaultAsyncConfigSASL(
	mechanism sarama.SASLMechanism,
	user string,
	password string,
) *sarama.Config {
	config := DefaultAsyncConfig()
	kafkaclient.PrepareDefaultConfigSASL(config, mechanism, user, password)

	return config
}

// DefaultSyncConfig returns the default configuration for SyncProducer.
func DefaultSyncConfig() *sarama.Config {
	config := sarama.NewConfig()
	config.ClientID = defaultClientID

	config.Version = kafkaclient.KafkaVersion
	config.Producer.Return.Successes = true // required for synchronous producer
	config.Producer.RequiredAcks = sarama.WaitForAll

	return config
}

// DefaultSyncConfigSASL returns the default configuration for AsyncProducer with SASL.
func DefaultSyncConfigSASL(
	mechanism sarama.SASLMechanism,
	user string,
	password string,
) *sarama.Config {
	config := DefaultAsyncConfig()
	kafkaclient.PrepareDefaultConfigSASL(config, mechanism, user, password)

	return config
}
