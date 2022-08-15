package conf

import (
	"fmt"
	"reflect"

	"github.com/squareup/pranadb/conf/tls"
	"github.com/squareup/pranadb/errors"
)

const (
	DefaultDataSnapshotEntries        = 50000
	DefaultDataCompactionOverhead     = 5000
	DefaultSequenceSnapshotEntries    = 1000
	DefaultSequenceCompactionOverhead = 50
	DefaultLocksSnapshotEntries       = 1000
	DefaultLocksCompactionOverhead    = 50
	DefaultRaftRTTMs                  = 50
	DefaultRaftHeartbeatRTT           = 30
	DefaultRaftElectionRTT            = 300
	DefaultAggregationCacheSizeRows   = 20000
	DefaultMaxProcessBatchSize        = 2000
	DefaultMaxForwardWriteBatchSize   = 500
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
	EnableAPIServer                  bool
	APIServerListenAddresses         []string
	APITLSConfig                     tls.TLSConfig `help:"API TLS configuration" embed:"" prefix:"api-tls-"`
	EnableSourceStats                bool
	ProtobufDescriptorDir            string `help:"Directory containing protobuf file descriptor sets that Prana should load to use for decoding Kafka messages. Filenames must end with .bin" type:"existingdir"`
	EnableLifecycleEndpoint          bool
	LifeCycleListenAddress           string
	StartupEndpointPath              string
	ReadyEndpointPath                string
	LiveEndpointPath                 string
	MetricsBind                      string `help:"Bind address for Prometheus metrics." default:"localhost:9102" env:"METRICS_BIND"`
	EnableMetrics                    bool
	EnableFailureInjector            bool
	ScreenDragonLogSpam              bool
	DisableShardPlacementSanityCheck bool
	RaftRTTMs                        int
	RaftElectionRTT                  int
	RaftHeartbeatRTT                 int
	DisableFsync                     bool
	DDProfilerTypes                  string
	DDProfilerHostEnvVarName         string
	DDProfilerPort                   int
	DDProfilerServiceName            string
	DDProfilerEnvironmentName        string
	DDProfilerVersionName            string
	AggregationCacheSizeRows         int // The maximum number of rows for an aggregation to cache in memory
	MaxProcessBatchSize              int
	MaxForwardWriteBatchSize         int
}

func (c *Config) ApplyDefaults() {
	if c.DataSnapshotEntries == 0 {
		c.DataSnapshotEntries = DefaultDataSnapshotEntries
	}
	if c.DataCompactionOverhead == 0 {
		c.DataCompactionOverhead = DefaultDataCompactionOverhead
	}
	if c.SequenceSnapshotEntries == 0 {
		c.SequenceSnapshotEntries = DefaultSequenceSnapshotEntries
	}
	if c.SequenceCompactionOverhead == 0 {
		c.SequenceCompactionOverhead = DefaultSequenceCompactionOverhead
	}
	if c.LocksSnapshotEntries == 0 {
		c.LocksSnapshotEntries = DefaultLocksSnapshotEntries
	}
	if c.LocksCompactionOverhead == 0 {
		c.LocksCompactionOverhead = DefaultLocksCompactionOverhead
	}
	if c.RaftRTTMs == 0 {
		c.RaftRTTMs = DefaultRaftRTTMs
	}
	if c.RaftHeartbeatRTT == 0 {
		c.RaftHeartbeatRTT = DefaultRaftHeartbeatRTT
	}
	if c.RaftElectionRTT == 0 {
		c.RaftElectionRTT = DefaultRaftElectionRTT
	}
	if c.AggregationCacheSizeRows == 0 {
		c.AggregationCacheSizeRows = DefaultAggregationCacheSizeRows
	}
	if c.MaxProcessBatchSize == 0 {
		c.MaxProcessBatchSize = DefaultMaxProcessBatchSize
	}
	if c.MaxForwardWriteBatchSize == 0 {
		c.MaxForwardWriteBatchSize = DefaultMaxForwardWriteBatchSize
	}
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
	if c.EnableAPIServer {
		if len(c.APIServerListenAddresses) == 0 {
			return errors.NewInvalidConfigurationError("APIServerListenAddresses must be specified")
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
	if c.MaxProcessBatchSize < 1 {
		return errors.NewInvalidConfigurationError("MaxProcessBatchSize must be > 0")
	}
	if c.MaxForwardWriteBatchSize < 1 {
		return errors.NewInvalidConfigurationError("MaxForwardWriteBatchSize must be > 0")
	}
	if !reflect.DeepEqual(c.APITLSConfig, tls.TLSConfig{}) {
		if err := c.APITLSConfig.ValidateTLSFiles(); err != nil {
			return errors.NewInvalidConfigurationError(fmt.Sprintf("TLS configuration error: %s", err.Error()))
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
	BrokerClientGenerator                    = 3
)

type BrokerConfig struct {
	ClientType BrokerClientType  `json:"client-type,omitempty"`
	Properties map[string]string `json:"properties,omitempty"`
}

func NewDefaultConfig() *Config {
	return &Config{
		DataSnapshotEntries:        DefaultDataSnapshotEntries,
		DataCompactionOverhead:     DefaultDataCompactionOverhead,
		SequenceSnapshotEntries:    DefaultSequenceSnapshotEntries,
		SequenceCompactionOverhead: DefaultSequenceCompactionOverhead,
		LocksSnapshotEntries:       DefaultLocksSnapshotEntries,
		LocksCompactionOverhead:    DefaultLocksCompactionOverhead,
		RaftRTTMs:                  DefaultRaftRTTMs,
		RaftHeartbeatRTT:           DefaultRaftHeartbeatRTT,
		RaftElectionRTT:            DefaultRaftElectionRTT,
		AggregationCacheSizeRows:   DefaultAggregationCacheSizeRows,
		MaxProcessBatchSize:        DefaultMaxProcessBatchSize,
		MaxForwardWriteBatchSize:   DefaultMaxForwardWriteBatchSize,
	}
}

func NewTestConfig(fakeKafkaID int64) *Config {
	return &Config{
		RaftRTTMs:                DefaultRaftRTTMs,
		RaftHeartbeatRTT:         DefaultRaftHeartbeatRTT,
		RaftElectionRTT:          DefaultRaftElectionRTT,
		AggregationCacheSizeRows: DefaultAggregationCacheSizeRows,
		MaxProcessBatchSize:      DefaultMaxProcessBatchSize,
		MaxForwardWriteBatchSize: DefaultMaxForwardWriteBatchSize,
		NodeID:                   0,
		NumShards:                10,
		TestServer:               true,
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
