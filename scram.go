package kafkaclient

import (
	"crypto/sha256"
	"crypto/sha512"

	"github.com/IBM/sarama"
	"github.com/xdg-go/scram"
)

var (
	// SHA256 - hash function for SCRAM.
	SHA256 scram.HashGeneratorFcn = sha256.New //nolint:gochecknoglobals // ok
	// SHA512 - hash function for SCRAM.
	SHA512 scram.HashGeneratorFcn = sha512.New //nolint:gochecknoglobals // ok
)

// XDGSCRAMClient implements sarama.SCRAMClient interface for SASL configuration
// https://github.com/IBM/sarama/blob/main/examples/sasl_scram_client/scram_client.go
type XDGSCRAMClient struct {
	*scram.Client
	*scram.ClientConversation
	scram.HashGeneratorFcn
}

var _ sarama.SCRAMClient = (*XDGSCRAMClient)(nil)

// NewXDGSCRAMClient creates a new instance of XDGSCRAMClient.
func NewXDGSCRAMClient(hashGeneratorFcn scram.HashGeneratorFcn) *XDGSCRAMClient {
	return &XDGSCRAMClient{
		HashGeneratorFcn: hashGeneratorFcn,
	}
}

// Begin starts SASL session.
func (x *XDGSCRAMClient) Begin(userName, password, authzID string) (err error) {
	x.Client, err = x.HashGeneratorFcn.NewClient(userName, password, authzID)
	if err != nil {
		return err
	}
	x.ClientConversation = x.Client.NewConversation()
	return nil
}

// Step performs one iteration of SASL session.
func (x *XDGSCRAMClient) Step(challenge string) (response string, err error) {
	return x.ClientConversation.Step(challenge)
}

// Done completes SASL session.
func (x *XDGSCRAMClient) Done() bool {
	return x.ClientConversation.Done()
}
