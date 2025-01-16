package realtest

import (
	"context"
	"testing"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/kafka"
)

// testKafkaContainer represents a Kafka container for testing.
type testKafkaContainer struct {
	container *kafka.KafkaContainer
	brokers   []string
}

// setupKafka starts a Kafka container and returns its configuration.
func setupKafka(t *testing.T) *testKafkaContainer {
	ctx := context.Background()
	container, err := kafka.Run(ctx,
		"confluentinc/cp-kafka:7.5.0",
		kafka.WithClusterID("test-cluster"),
	)
	require.NoError(t, err, "Failed to start Kafka container")

	brokers, err := container.Brokers(ctx)
	require.NoError(t, err, "Failed to get broker list")

	return &testKafkaContainer{
		container: container,
		brokers:   brokers,
	}
}

// createTopic creates a test topic with specified partitions.
func (k *testKafkaContainer) createTopic(t *testing.T, topic string, partitions int32) {
	config := sarama.NewConfig()
	config.Version = sarama.V3_5_0_0

	admin, err := sarama.NewClusterAdmin(k.brokers, config)
	require.NoError(t, err, "Failed to create cluster admin")
	defer func() { _ = admin.Close() }()

	err = admin.CreateTopic(topic, &sarama.TopicDetail{
		NumPartitions:     partitions,
		ReplicationFactor: 1,
	}, false)
	require.NoError(t, err, "Failed to create topic")
}

// cleanup terminates the Kafka container.
func (k *testKafkaContainer) cleanup(t *testing.T) {
	ctx := context.Background()
	err := k.container.Terminate(ctx)
	require.NoError(t, err, "Failed to terminate Kafka container")
}
