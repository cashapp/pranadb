package kafkatest

import (
	"testing"
	"time"

	"github.com/ory/dockertest"
	dc "github.com/ory/dockertest/docker"
	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	redpandVersion = "v21.7.6"

	kafkaAPIPort  = "9092"
	zookeeperPort = "2181"
)

// KafkaContainer is a reference to the docker kafka running Kafka (or RedPanda).
type KafkaContainer struct {
	t         *testing.T
	Topic     string
	Addr      string
	kafka     *dockertest.Resource
	zookeeper *dockertest.Resource
}

// Stop the container.
func (c *KafkaContainer) Stop() {
	if c == nil {
		return
	}
	if c.kafka != nil {
		assert.NoError(c.t, c.kafka.Close())
	}
	if c.zookeeper != nil {
		assert.NoError(c.t, c.zookeeper.Close())
	}
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

// RequireKafka runs Kafka along with Zookeeper, and creates the requested topic.
// If Kafka API is already healthy on kafkaAPIPort 9092, the containers will not be created.
// The requested topic will still be created.
func RequireKafka(t *testing.T, topicName string, numPartitions int) *KafkaContainer {
	t.Helper()

	var zk, kfk *dockertest.Resource
	if err := checkKafkaHealth(kafkaAPIPort); err == nil {
		log.Warn("Kafka already running on kafkaAPIPort " + kafkaAPIPort)
	} else {
		zk, kfk = runKafka(t)
	}

	conn, err := kafka.Dial("tcp", ":"+kafkaAPIPort)
	require.NoError(t, err)
	require.NoError(t, conn.CreateTopics(kafka.TopicConfig{Topic: topicName, NumPartitions: numPartitions, ReplicationFactor: -1}))

	return &KafkaContainer{
		t:         t,
		Addr:      ":" + kafkaAPIPort,
		Topic:     topicName,
		zookeeper: zk,
		kafka:     kfk,
	}
}

func runKafka(t *testing.T) (zk, kafka *dockertest.Resource) {
	t.Helper()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 2 * time.Minute

	docker, err := dc.NewClientFromEnv()
	require.NoError(t, err)
	network, err := docker.CreateNetwork(dc.CreateNetworkOptions{
		Name:   "prana",
		Driver: "bridge",
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := docker.RemoveNetwork(network.ID); err != nil {
			log.Errorf("Failed to remove docker network `prana`: %v", err)
		}
	})

	log.Info("Starting Zookeeper on :" + zookeeperPort)
	log.Info("Run `docker logs -f zookeeper` for logs")
	zk, err = pool.RunWithOptions(&dockertest.RunOptions{
		Name:         "zookeeper",
		Repository:   "wurstmeister/zookeeper",
		Tag:          "latest",
		ExposedPorts: []string{zookeeperPort},
		PortBindings: map[dc.Port][]dc.PortBinding{
			zookeeperPort: {{HostPort: zookeeperPort}},
		},
		NetworkID: network.ID,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := zk.Close(); err != nil {
			t.Logf("failed to stop zookeeper: %v", err)
		}
	})

	log.Info("Starting Kafka on :" + zookeeperPort)
	log.Info("Run `docker logs -f kafka` for logs")
	kafka, err = pool.RunWithOptions(&dockertest.RunOptions{
		Name:       "kafka",
		Repository: "wurstmeister/kafka",
		Tag:        "2.12-2.5.0",
		Env: []string{
			"KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:" + kafkaAPIPort,
			"KAFKA_LISTENERS=PLAINTEXT://:" + kafkaAPIPort,
			"KAFKA_ZOOKEEPER_CONNECT=zookeeper:" + zookeeperPort,
		},
		ExposedPorts: []string{kafkaAPIPort},
		PortBindings: map[dc.Port][]dc.PortBinding{
			kafkaAPIPort: {{HostPort: kafkaAPIPort}},
		},
		NetworkID: network.ID,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := kafka.Close(); err != nil {
			t.Logf("failed to stop kafka: %v", err)
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

	return zk, kafka
}

func checkKafkaHealth(port string) error { // nolint: unparam
	conn, err := kafka.Dial("tcp", ":"+port)
	if err != nil {
		return err
	}
	// Read metadata to ensure Kafka is available.
	_, err = conn.Brokers()
	return err
}
