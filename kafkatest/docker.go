package kafkatest

import (
	"testing"
	"time"

	"github.com/ory/dockertest"
	dc "github.com/ory/dockertest/docker"
	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

const redpandVersion = "v21.7.6"
const kafkaAPIPort = "9092"

// KafkaContainer is a reference to the docker kafka running Kafka (or RedPanda).
type KafkaContainer struct {
	t     *testing.T
	Topic string
	Addr  string
	kafka *dockertest.Resource
}

// Stop the container.
func (c *KafkaContainer) Stop() error {
	if c == nil || c.kafka == nil {
		return nil
	}
	return c.kafka.Close()
}

// RequireRedPanda runs RedPanda to serve as Kafka and create the requested topic. RedPanda is API-compatible
// with Kafka and significantly faster. If Kafka API is already healthy on kafkaAPIPort 9092, RedPanda will not be created.
// The requested topic will still be created.
func RequireRedPanda(t *testing.T, topicName string, numPartitions int) *KafkaContainer {
	t.Helper()

	var container *dockertest.Resource
	if err := checkKafkaHealth(kafkaAPIPort); err == nil {
		log.Warn("Kafka already running on kafkaAPIPort " + kafkaAPIPort)
	} else {
		container = runRedPanda(t)
	}

	conn, err := kafka.Dial("tcp", ":"+kafkaAPIPort)
	require.NoError(t, err)
	require.NoError(t, conn.CreateTopics(kafka.TopicConfig{Topic: topicName, NumPartitions: numPartitions, ReplicationFactor: -1}))

	return &KafkaContainer{
		t:     t,
		Addr:  ":" + kafkaAPIPort,
		Topic: topicName,
		kafka: container,
	}
}

func runRedPanda(t *testing.T) *dockertest.Resource {
	t.Helper()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 60 * time.Second

	log.Info("Starting RedPanda on :" + kafkaAPIPort)
	log.Info("Run `docker logs -f redpanda` for logs")
	container, err := pool.RunWithOptions(&dockertest.RunOptions{
		Name:       "redpanda",
		Repository: "docker.vectorized.io/vectorized/redpanda",
		Tag:        redpandVersion,
		Cmd: []string{
			"redpanda",
			"start",
			"--overprovisioned",
			"--smp 1 ",
			"--memory 1G",
			"--reserve-memory 0M",
			"--node-id 0",
			"--check=false",
		},
		ExposedPorts: []string{kafkaAPIPort},
		PortBindings: map[dc.Port][]dc.PortBinding{
			kafkaAPIPort: {{HostPort: kafkaAPIPort}},
		},
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		if err := container.Close(); err != nil {
			t.Logf("failed to stop redpanda kafka: %v", err)
		}
	})

	err = pool.Retry(func() error {
		err := checkKafkaHealth(kafkaAPIPort)
		if err != nil {
			log.Infof("kafka connection not ready: %v", err)
		}
		return err
	})
	require.NoError(t, err)

	return container
}

func checkKafkaHealth(port string) error {
	conn, err := kafka.Dial("tcp", ":"+port)
	if err != nil {
		return err
	}
	// Read metadata to ensure Kafka is available.
	_, err = conn.Brokers()
	return err
}
