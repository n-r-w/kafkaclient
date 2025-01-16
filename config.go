package kafkaclient

import (
	"fmt"
	"regexp"

	"github.com/IBM/sarama"
)

// KafkaVersion is the default Kafka version.
var KafkaVersion = sarama.V3_4_0_0 //nolint:gochecknoglobals // ok

var validClientID = regexp.MustCompile(`\A[A-Za-z0-9._-]+\z`)

// ValidateClientID checks the validity of ClientID for Kafka.
func ValidateClientID(clientID string) error {
	if !validClientID.MatchString(clientID) {
		return fmt.Errorf(
			"invalid client ID: %q. Must be a non-empty string with only letters, numbers, dots, underscores and dashes",
			clientID)
	}

	return nil
}

var saslMechanismMap = map[string]sarama.SASLMechanism{ //nolint:gochecknoglobals // ok
	"PLAIN":       sarama.SASLTypePlaintext,
	"SHA512":      sarama.SASLTypeSCRAMSHA512,
	"SHA256":      sarama.SASLTypeSCRAMSHA256,
	"OAUTHBEARER": sarama.SASLTypeOAuth,
	"GSSAPI":      sarama.SASLTypeGSSAPI,
}

// SaslMechanismFromString converts a config string to the corresponding SASL type in sarama.
// Can be helpful for config parsing.
func SaslMechanismFromString(saslMechanism string) (sarama.SASLMechanism, error) {
	if m, ok := saslMechanismMap[saslMechanism]; ok {
		return m, nil
	}

	return "", fmt.Errorf("invalid sasl mechanism: %s", saslMechanism)
}

// PrepareDefaultConfigSASL prepares the configuration for creating a producer or consumer with SASL.
func PrepareDefaultConfigSASL(
	config *sarama.Config,
	mechanism sarama.SASLMechanism,
	user string,
	password string,
) {
	config.Net.SASL.Enable = true
	config.Net.SASL.Handshake = true
	config.Net.SASL.Mechanism = mechanism
	config.Net.SASL.User = user
	config.Net.SASL.Password = password

	if mechanism == sarama.SASLTypeSCRAMSHA256 {
		config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
			return NewXDGSCRAMClient(SHA256)
		}
	} else if mechanism == sarama.SASLTypeSCRAMSHA512 {
		config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
			return NewXDGSCRAMClient(SHA512)
		}
	}
}
