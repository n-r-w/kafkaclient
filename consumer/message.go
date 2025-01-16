package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/IBM/sarama"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// RecordHeader represents a Kafka message header.
type RecordHeader struct {
	Key   []byte
	Value []byte
}

// Message represents a message for processing in the consumer.
type Message struct {
	session sarama.ConsumerGroupSession
	message *sarama.ConsumerMessage
	lag     int64
}

// Payload returns the message as sarama.ConsumerMessage.
func (m *Message) Payload() *sarama.ConsumerMessage {
	return m.message
}

// Headers returns the message headers.
func (m *Message) Headers() []RecordHeader {
	headers := make([]RecordHeader, 0, len(m.message.Headers))
	for _, h := range m.message.Headers {
		headers = append(headers, RecordHeader{
			Key:   h.Key,
			Value: h.Value,
		})
	}
	return headers
}

// Timestamp inner message timestamp.
func (m *Message) Timestamp() time.Time {
	return m.message.Timestamp
}

// BlockTimestamp outer (compressed) block timestamp.
func (m *Message) BlockTimestamp() time.Time {
	return m.message.BlockTimestamp
}

// Key returns the message key.
func (m *Message) Key() []byte {
	return m.message.Key
}

// Value returns the message value.
func (m *Message) Value() []byte {
	return m.message.Value
}

// Topic returns the message topic.
func (m *Message) Topic() string {
	return m.message.Topic
}

// Partition returns the message partition.
func (m *Message) Partition() int32 {
	return m.message.Partition
}

// Offset returns the message offset.
func (m *Message) Offset() int64 {
	return m.message.Offset
}

// MemberID returns the cluster member ID.
func (m *Message) MemberID() string {
	return m.session.MemberID()
}

// GenerationID returns the current generation ID.
func (m *Message) GenerationID() int32 {
	return m.session.GenerationID()
}

// Lag returns the message lag.
func (m *Message) Lag() int64 {
	return m.lag
}

// Context returns the message context
// The context of an individual message may differ from the context passed to ConsumeKafkaMessages.
func (m *Message) Context() context.Context {
	return m.session.Context()
}

// ReadInProto deserializes the message into a proto message.
func (m *Message) ReadInProto(mProto protoreflect.ProtoMessage) error {
	if err := proto.Unmarshal(m.message.Value, mProto); err != nil {
		return fmt.Errorf("failed to unmarshal proto message: %w", err)
	}

	return nil
}

// ReadInJSON deserializes the message into JSON.
func (m *Message) ReadInJSON(mJSON any) error {
	if err := json.Unmarshal(m.message.Value, mJSON); err != nil {
		return fmt.Errorf("failed to unmarshal json message: %w", err)
	}

	return nil
}
