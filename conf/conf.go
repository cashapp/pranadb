package conf

import (
	"fmt"
	"time"

	"github.com/squareup/pranadb/errors"
)

const (
	DefaultDataSnapshotEntries           = 10000
	DefaultDataCompactionOverhead        = 2500
	DefaultSequenceSnapshotEntries       = 1000
	DefaultSequenceCompactionOverhead    = 250
	DefaultLocksSnapshotEntries          = 1000
	DefaultLocksCompactionOverhead       = 250
	DefaultRemotingHeartbeatInterval     = 10 * time.Second
	DefaultRemotingHeartbeatTimeout      = 5 * time.Second
	DefaultAPIServerSessionTimeout       = 30 * time.Second
	DefaultAPIServerSessionCheckInterval = 5 * time.Second
	DefaultGlobalIngestLimitRowsPerSec   = 1000
	DefaultRaftRTTMs                     = 100
	DefaultRaftHeartbeatRTT              = 30
	DefaultRaftElectionRTT               = 300
)

type Config struct {
	NodeID                           int
	ClusterID                        uint64 // All nodes in a Prana cluster must share the same ClusterID
	RaftAddresses                    []string
	NotifListenAddresses             []string
	NumShards                        int
	ReplicationFactor                int
	DataDir                          string
	TestServer                       bool
	KafkaBrokers                     BrokerConfigs
	DataSnapshotEntries              int
	DataCompactionOverhead           int
	SequenceSnapshotEntries          int
	SequenceCompactionOverhead       int
	LocksSnapshotEntries             int
	LocksCompactionOverhead          int
	RemotingHeartbeatInterval        time.Duration
	RemotingHeartbeatTimeout         time.Duration
	EnableAPIServer                  bool
	APIServerListenAddresses         []string
	APIServerSessionTimeout          time.Duration
	APIServerSessionCheckInterval    time.Duration
	EnableSourceStats                bool
	ProtobufDescriptorDir            string `help:"Directory containing protobuf file descriptor sets that Prana should load to use for decoding Kafka messages. Filenames must end with .bin" type:"existingdir"`
	EnableLifecycleEndpoint          bool
	LifeCycleListenAddress           string
	StartupEndpointPath              string
	ReadyEndpointPath                string
	LiveEndpointPath                 string
	MetricsBind                      string `help:"Bind address for Prometheus metrics." default:"localhost:9102" env:"METRICS_BIND"`
	EnableMetrics                    bool
	GlobalIngestLimitRowsPerSec      int
	EnableFailureInjector            bool
	ScreenDragonLogSpam              bool
	DisableShardPlacementSanityCheck bool
	RaftRTTMs                        int
	RaftElectionRTT                  int
	RaftHeartbeatRTT                 int
}

func (c *Config) Validate() error { //nolint:gocyclo
	if c.NodeID < 0 {
		return errors.NewInvalidConfigurationError("NodeID must be >= 0")
	}
	if c.ClusterID < 0 {
		return errors.NewInvalidConfigurationError("ClusterID must be >= 0")
	}
	if c.NumShards < 1 {
		return errors.NewInvalidConfigurationError("NumShards must be >= 1")
	}
	if len(c.KafkaBrokers) == 0 {
		return errors.NewInvalidConfigurationError("KafkaBrokers must be specified")
	}
	for bName, kb := range c.KafkaBrokers {
		if kb.ClientType == BrokerClientTypeUnknown {
			return errors.NewInvalidConfigurationError(fmt.Sprintf("KafkaBroker %s, invalid ClientType, must be %d or %d",
				bName, BrokerClientFake, BrokerClientDefault))
		}
	}
	if c.RemotingHeartbeatInterval < 1*time.Second {
		return errors.NewInvalidConfigurationError(fmt.Sprintf("RemotingHeartbeatInterval must be >= %d", time.Second))
	}
	if c.RemotingHeartbeatTimeout < 1*time.Millisecond {
		return errors.NewInvalidConfigurationError(fmt.Sprintf("RemotingHeartbeatTimeout must be >= %d", time.Millisecond))
	}
	if c.EnableAPIServer {
		if len(c.APIServerListenAddresses) == 0 {
			return errors.NewInvalidConfigurationError("APIServerListenAddresses must be specified")
		}
		if c.APIServerSessionTimeout < 1*time.Second {
			return errors.NewInvalidConfigurationError(fmt.Sprintf("APIServerSessionTimeout must be >= %d", 1*time.Second))
		}
		if c.APIServerSessionCheckInterval < 100*time.Millisecond {
			return errors.NewInvalidConfigurationError(fmt.Sprintf("APIServerSessionCheckInterval must be >= %d", 100*time.Millisecond))
		}
	}
	if !c.TestServer {
		if c.NodeID >= len(c.RaftAddresses) {
			return errors.NewInvalidConfigurationError("NodeID must be in the range 0 (inclusive) to len(RaftAddresses) (exclusive)")
		}
		if c.DataDir == "" {
			return errors.NewInvalidConfigurationError("DataDir must be specified")
		}
		if c.ReplicationFactor < 3 {
			return errors.NewInvalidConfigurationError("ReplicationFactor must be >= 3")
		}
		if len(c.RaftAddresses) < c.ReplicationFactor {
			return errors.NewInvalidConfigurationError("Number of RaftAddresses must be >= ReplicationFactor")
		}
		if len(c.NotifListenAddresses) != len(c.RaftAddresses) {
			return errors.NewInvalidConfigurationError("Number of RaftAddresses must be same as number of NotifListenerAddresses")
		}
		if c.EnableAPIServer && len(c.APIServerListenAddresses) != len(c.RaftAddresses) {
			return errors.NewInvalidConfigurationError("Number of RaftAddresses must be same as number of APIServerListenAddresses")
		}
		if c.DataSnapshotEntries < 10 {
			return errors.NewInvalidConfigurationError("DataSnapshotEntries must be >= 10")
		}
		if c.DataCompactionOverhead < 5 {
			return errors.NewInvalidConfigurationError("DataCompactionOverhead must be >= 5")
		}
		if c.DataCompactionOverhead > c.DataSnapshotEntries {
			return errors.NewInvalidConfigurationError("DataSnapshotEntries must be >= DataCompactionOverhead")
		}
		if c.SequenceSnapshotEntries < 10 {
			return errors.NewInvalidConfigurationError("SequenceSnapshotEntries must be >= 10")
		}
		if c.SequenceCompactionOverhead < 5 {
			return errors.NewInvalidConfigurationError("SequenceCompactionOverhead must be >= 5")
		}
		if c.SequenceCompactionOverhead > c.SequenceSnapshotEntries {
			return errors.NewInvalidConfigurationError("SequenceSnapshotEntries must be >= SequenceCompactionOverhead")
		}
		if c.LocksSnapshotEntries < 10 {
			return errors.NewInvalidConfigurationError("LocksSnapshotEntries must be >= 10")
		}
		if c.LocksCompactionOverhead < 5 {
			return errors.NewInvalidConfigurationError("LocksCompactionOverhead must be >= 5")
		}
		if c.LocksCompactionOverhead > c.LocksSnapshotEntries {
			return errors.NewInvalidConfigurationError("LocksSnapshotEntries must be >= LocksCompactionOverhead")
		}
	}
	if c.EnableLifecycleEndpoint {
		if c.LifeCycleListenAddress == "" {
			return errors.NewInvalidConfigurationError("LifeCycleListenAddress must be specified")
		}
		if c.StartupEndpointPath == "" {
			return errors.NewInvalidConfigurationError("StartupEndpointPath must be specified")
		}
		if c.LiveEndpointPath == "" {
			return errors.NewInvalidConfigurationError("LiveEndpointPath must be specified")
		}
		if c.ReadyEndpointPath == "" {
			return errors.NewInvalidConfigurationError("ReadyEndpointPath must be specified")
		}
	}
	if c.GlobalIngestLimitRowsPerSec < -1 || c.GlobalIngestLimitRowsPerSec == 0 {
		return errors.NewInvalidConfigurationError("GlobalIngestLimitRowsPerSec must be > 0 or -1")
	}
	if c.RaftRTTMs < 1 {
		return errors.NewInvalidConfigurationError("RaftRTTMs must be > 0")
	}
	if c.RaftHeartbeatRTT < 1 {
		return errors.NewInvalidConfigurationError("RaftHeartbeatRTT must be > 0")
	}
	if c.RaftElectionRTT < 1 {
		return errors.NewInvalidConfigurationError("RaftElectionRTT must be > 0")
	}
	if c.RaftElectionRTT < 2*c.RaftHeartbeatRTT {
		return errors.NewInvalidConfigurationError("RaftElectionRTT must be > 2 * RaftHeartbeatRTT")
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
	ClientType BrokerClientType  `json:"client-type,omitempty"`
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
		RemotingHeartbeatInterval:     DefaultRemotingHeartbeatInterval,
		RemotingHeartbeatTimeout:      DefaultRemotingHeartbeatTimeout,
		APIServerSessionTimeout:       DefaultAPIServerSessionTimeout,
		APIServerSessionCheckInterval: DefaultAPIServerSessionCheckInterval,
		GlobalIngestLimitRowsPerSec:   DefaultGlobalIngestLimitRowsPerSec,
		RaftRTTMs:                     DefaultRaftRTTMs,
		RaftHeartbeatRTT:              DefaultRaftHeartbeatRTT,
		RaftElectionRTT:               DefaultRaftElectionRTT,
	}
}

func NewTestConfig(fakeKafkaID int64) *Config {
	return &Config{
		RemotingHeartbeatInterval:     DefaultRemotingHeartbeatInterval,
		RemotingHeartbeatTimeout:      DefaultRemotingHeartbeatTimeout,
		APIServerSessionTimeout:       DefaultAPIServerSessionTimeout,
		APIServerSessionCheckInterval: DefaultAPIServerSessionCheckInterval,
		GlobalIngestLimitRowsPerSec:   DefaultGlobalIngestLimitRowsPerSec,
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
