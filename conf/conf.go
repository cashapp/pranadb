package conf

import (
	"fmt"
	"time"

	"github.com/squareup/pranadb/perrors"
)

const (
	DefaultDataSnapshotEntries           = 10000
	DefaultDataCompactionOverhead        = 2500
	DefaultSequenceSnapshotEntries       = 1000
	DefaultSequenceCompactionOverhead    = 250
	DefaultLocksSnapshotEntries          = 1000
	DefaultLocksCompactionOverhead       = 250
	DefaultNotifierHeartbeatInterval     = 5 * time.Second
	DefaultAPIServerSessionTimeout       = 30 * time.Second
	DefaultAPIServerSessionCheckInterval = 5 * time.Second
)

type Config struct {
	NodeID                        int           `json:"node_id,omitempty"`
	ClusterID                     int           `json:"cluster_id,omitempty"` // All nodes in a Prana cluster must share the same ClusterID
	RaftAddresses                 []string      `json:"raft_addresses,omitempty"`
	NotifListenAddresses          []string      `json:"notif_listen_addresses,omitempty"`
	NumShards                     int           `json:"num_shards,omitempty"`
	ReplicationFactor             int           `json:"replication_factor,omitempty"`
	DataDir                       string        `json:"data_dir,omitempty"`
	TestServer                    bool          `json:"test_server,omitempty"`
	KafkaBrokers                  BrokerConfigs `json:"kafka_brokers,omitempty"`
	DataSnapshotEntries           int           `json:"data_snapshot_entries,omitempty"`
	DataCompactionOverhead        int           `json:"data_compaction_overhead,omitempty"`
	SequenceSnapshotEntries       int           `json:"sequence_snapshot_entries,omitempty"`
	SequenceCompactionOverhead    int           `json:"sequence_compaction_overhead,omitempty"`
	LocksSnapshotEntries          int           `json:"locks_snapshot_entries,omitempty"`
	LocksCompactionOverhead       int           `json:"locks_compaction_overhead,omitempty"`
	Debug                         bool          `json:"debug,omitempty"`
	NotifierHeartbeatInterval     time.Duration `json:"notifier_heartbeat_interval,omitempty"`
	EnableAPIServer               bool          `json:"enable_api_server,omitempty"`
	APIServerListenAddresses      []string      `json:"api_server_listen_addresses,omitempty"`
	APIServerSessionTimeout       time.Duration `json:"api_server_session_timeout,omitempty"`
	APIServerSessionCheckInterval time.Duration `json:"api_server_session_check_interval,omitempty"`
}

func (c *Config) Validate() error { //nolint:gocyclo
	if c.NodeID < 0 {
		return perrors.NewInvalidConfigurationError("NodeID must be >= 0")
	}
	if c.ClusterID < 0 {
		return perrors.NewInvalidConfigurationError("ClusterID must be >= 0")
	}
	if c.NumShards < 1 {
		return perrors.NewInvalidConfigurationError("NumShards must be >= 1")
	}
	if len(c.KafkaBrokers) == 0 {
		return perrors.NewInvalidConfigurationError("KafkaBrokers must be specified")
	}
	for bName, kb := range c.KafkaBrokers {
		if kb.ClientType == BrokerClientTypeUnknown {
			return perrors.NewInvalidConfigurationError(fmt.Sprintf("KafkaBroker %s, invalid ClientType, must be %d or %d",
				bName, BrokerClientFake, BrokerClientDefault))
		}
	}
	if c.NotifierHeartbeatInterval < 1*time.Second {
		return perrors.NewInvalidConfigurationError(fmt.Sprintf("NotifierHeartbeatInterval must be >= %d", time.Second))
	}
	if c.EnableAPIServer {
		if len(c.APIServerListenAddresses) == 0 {
			return perrors.NewInvalidConfigurationError("APIServerListenAddresses must be specified")
		}
		if c.APIServerSessionTimeout < 1*time.Second {
			return perrors.NewInvalidConfigurationError(fmt.Sprintf("APIServerSessionTimeout must be >= %d", 1*time.Second))
		}
		if c.APIServerSessionCheckInterval < 100*time.Millisecond {
			return perrors.NewInvalidConfigurationError(fmt.Sprintf("APIServerSessionCheckInterval must be >= %d", 100*time.Millisecond))
		}
	}
	if !c.TestServer {
		if c.NodeID >= len(c.RaftAddresses) {
			return perrors.NewInvalidConfigurationError("NodeID must be in the range 0 (inclusive) to len(RaftAddresses) (exclusive)")
		}
		if c.DataDir == "" {
			return perrors.NewInvalidConfigurationError("DataDir must be specified")
		}
		if c.ReplicationFactor < 3 {
			return perrors.NewInvalidConfigurationError("ReplicationFactor must be >= 3")
		}
		if len(c.RaftAddresses) < c.ReplicationFactor {
			return perrors.NewInvalidConfigurationError("Number of RaftAddresses must be >= ReplicationFactor")
		}
		if len(c.NotifListenAddresses) != len(c.RaftAddresses) {
			return perrors.NewInvalidConfigurationError("Number of RaftAddresses must be same as number of NotifListenerAddresses")
		}
		if c.EnableAPIServer && len(c.APIServerListenAddresses) != len(c.RaftAddresses) {
			return perrors.NewInvalidConfigurationError("Number of RaftAddresses must be same as number of APIServerListenAddresses")
		}
		if c.DataSnapshotEntries < 10 {
			return perrors.NewInvalidConfigurationError("DataSnapshotEntries must be >= 10")
		}
		if c.DataCompactionOverhead < 5 {
			return perrors.NewInvalidConfigurationError("DataCompactionOverhead must be >= 5")
		}
		if c.DataCompactionOverhead > c.DataSnapshotEntries {
			return perrors.NewInvalidConfigurationError("DataSnapshotEntries must be >= DataCompactionOverhead")
		}
		if c.SequenceSnapshotEntries < 10 {
			return perrors.NewInvalidConfigurationError("SequenceSnapshotEntries must be >= 10")
		}
		if c.SequenceCompactionOverhead < 5 {
			return perrors.NewInvalidConfigurationError("SequenceCompactionOverhead must be >= 5")
		}
		if c.SequenceCompactionOverhead > c.SequenceSnapshotEntries {
			return perrors.NewInvalidConfigurationError("SequenceSnapshotEntries must be >= SequenceCompactionOverhead")
		}
		if c.LocksSnapshotEntries < 10 {
			return perrors.NewInvalidConfigurationError("LocksSnapshotEntries must be >= 10")
		}
		if c.LocksCompactionOverhead < 5 {
			return perrors.NewInvalidConfigurationError("LocksCompactionOverhead must be >= 5")
		}
		if c.LocksCompactionOverhead > c.LocksSnapshotEntries {
			return perrors.NewInvalidConfigurationError("LocksSnapshotEntries must be >= LocksCompactionOverhead")
		}
	}
	return nil
}

type BrokerConfigs map[string]BrokerConfig // Key is broker name which is referred to in the source descriptor

type BrokerClientType int

const (
	BrokerClientTypeUnknown                  = 0
	BrokerClientFake        BrokerClientType = 1
	BrokerClientDefault                      = 2
)

type BrokerConfig struct {
	ClientType BrokerClientType  `json:"client_type,omitempty"`
	Properties map[string]string `json:"properties,omitempty"`
}

func NewDefaultConfig() *Config {
	return &Config{
		DataSnapshotEntries:           DefaultDataSnapshotEntries,
		DataCompactionOverhead:        DefaultDataCompactionOverhead,
		SequenceSnapshotEntries:       DefaultSequenceSnapshotEntries,
		SequenceCompactionOverhead:    DefaultSequenceCompactionOverhead,
		LocksSnapshotEntries:          DefaultLocksSnapshotEntries,
		LocksCompactionOverhead:       DefaultLocksCompactionOverhead,
		NotifierHeartbeatInterval:     DefaultNotifierHeartbeatInterval,
		APIServerSessionTimeout:       DefaultAPIServerSessionTimeout,
		APIServerSessionCheckInterval: DefaultAPIServerSessionCheckInterval,
	}
}

func NewTestConfig(fakeKafkaID int64) *Config {
	return &Config{
		NotifierHeartbeatInterval:     DefaultNotifierHeartbeatInterval,
		APIServerSessionTimeout:       DefaultAPIServerSessionTimeout,
		APIServerSessionCheckInterval: DefaultAPIServerSessionCheckInterval,
		NodeID:                        0,
		NumShards:                     10,
		TestServer:                    true,
		KafkaBrokers: BrokerConfigs{
			"testbroker": BrokerConfig{
				ClientType: BrokerClientFake,
				Properties: map[string]string{
					"fakeKafkaID": fmt.Sprintf("%d", fakeKafkaID),
				},
			},
		},
	}
}
