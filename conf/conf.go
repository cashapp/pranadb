package conf

import (
	"fmt"
	"time"

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
	DefaultRaftCallTimeout            = 15 * time.Second
	DefaultAggregationCacheSizeRows   = 20000
	DefaultMaxProcessBatchSize        = 2000
	DefaultMaxForwardWriteBatchSize   = 500
	DefaultMaxTableReaperBatchSize    = 1000
)

type Config struct {
	NodeID                       int
	ClusterID                    uint64 // All nodes in a Prana cluster must share the same ClusterID
	RaftListenAddresses          []string
	IntraClusterTLSConfig        TLSConfig `embed:"" prefix:"intra-cluster-tls-"`
	RemotingListenAddresses      []string
	NumShards                    int
	ReplicationFactor            int
	DataDir                      string
	TestServer                   bool
	KafkaBrokers                 BrokerConfigs
	DataSnapshotEntries          int
	DataCompactionOverhead       int
	SequenceSnapshotEntries      int
	SequenceCompactionOverhead   int
	LocksSnapshotEntries         int
	LocksCompactionOverhead      int
	GRPCAPIServerEnabled         bool      `name:"grpc-api-server-enabled"`
	GRPCAPIServerListenAddresses []string  `name:"grpc-api-server-listen-addresses"`
	GRPCAPIServerTLSConfig       TLSConfig `embed:"" prefix:"grpc-api-server-tls-"`
	HTTPAPIServerEnabled         bool      `name:"http-api-server-enabled"`
	HTTPAPIServerListenAddresses []string  `name:"http-api-server-listen-addresses"`
	HTTPAPIServerTLSConfig       TLSConfig `embed:"" prefix:"http-api-server-tls-"`
	SourceStatsEnabled           bool
	ProtobufDescriptorDir        string `help:"Directory containing protobuf file descriptor sets that Prana should load to use for decoding Kafka messages. Filenames must end with .bin" type:"existingdir"`
	LifecycleEndpointEnabled     bool   `name:"lifecycle-endpoint-enabled"`
	LifeCycleListenAddress       string
	StartupEndpointPath          string
	ReadyEndpointPath            string
	LiveEndpointPath             string
	MetricsBind                  string `help:"Bind address for Prometheus metrics." default:"localhost:9102" env:"METRICS_BIND"`
	MetricsEnabled               bool
	FailureInjectorEnabled       bool
	ScreenDragonLogSpam          bool
	RaftRTTMs                    int
	RaftElectionRTT              int
	RaftHeartbeatRTT             int
	RaftCallTimeout              time.Duration
	DisableFsync                 bool
	DDProfilerTypes              string
	DDProfilerHostEnvVarName     string
	DDProfilerPort               int
	DDProfilerServiceName        string
	DDProfilerEnvironmentName    string
	DDProfilerVersionName        string
	AggregationCacheSizeRows     int // The maximum number of rows for an aggregation to cache in memory
	MaxProcessBatchSize          int
	MaxForwardWriteBatchSize     int
	MaxTableReaperBatchSize      int
}

type TLSConfig struct {
	Enabled         bool   `help:"Set to true to enable TLS. TLS must be enabled when using the HTTP 2 API" default:"false"`
	KeyPath         string `help:"Path to a PEM encoded file containing the server private key"`
	CertPath        string `help:"Path to a PEM encoded file containing the server certificate"`
	ClientCertsPath string `help:"Path to a PEM encoded file containing trusted client certificates and/or CA certificates. Only needed with TLS client authentication"`
	ClientAuth      string `help:"Client certificate authentication mode. One of: no-client-cert, request-client-cert, require-any-client-cert, verify-client-cert-if-given, require-and-verify-client-cert"`
}

type ClientAuthMode string

const (
	ClientAuthModeUnspecified                = ""
	ClientAuthModeNoClientCert               = "no-client-cert"
	ClientAuthModeRequestClientCert          = "request-client-cert"
	ClientAuthModeRequireAnyClientCert       = "require-any-client-cert"
	ClientAuthModeVerifyClientCertIfGiven    = "verify-client-cert-if-given"
	ClientAuthModeRequireAndVerifyClientCert = "require-and-verify-client-cert"
)

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
	if c.HTTPAPIServerEnabled {
		c.HTTPAPIServerTLSConfig.Enabled = true
	}
	if c.RaftCallTimeout == 0 {
		c.RaftCallTimeout = DefaultRaftCallTimeout
	}
	if c.MaxTableReaperBatchSize == 0 {
		c.MaxTableReaperBatchSize = DefaultMaxTableReaperBatchSize
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
	if c.HTTPAPIServerEnabled {
		if len(c.HTTPAPIServerListenAddresses) == 0 {
			return errors.NewInvalidConfigurationError("HTTPAPIServerListenAddresses must be specified")
		}
		if !c.HTTPAPIServerTLSConfig.Enabled {
			return errors.NewInvalidConfigurationError("HTTPAPIServerTLSConfig.Enabled must be true if the HTTP API server is enabled")
		}
		if c.HTTPAPIServerTLSConfig.CertPath == "" {
			return errors.NewInvalidConfigurationError("HTTPAPIServerTLSConfig.CertPath must be specified for HTTP API server")
		}
		if c.HTTPAPIServerTLSConfig.KeyPath == "" {
			return errors.NewInvalidConfigurationError("HTTPAPIServerTLSConfig.KeyPath must be specified for HTTP API server")
		}
		if c.HTTPAPIServerTLSConfig.ClientAuth != "" && c.HTTPAPIServerTLSConfig.ClientCertsPath == "" {
			return errors.NewInvalidConfigurationError("HTTPAPIServerTLSConfig.ClientCertsPath must be provided if client auth is enabled")
		}
	}
	if c.GRPCAPIServerEnabled {
		if len(c.GRPCAPIServerListenAddresses) == 0 {
			return errors.NewInvalidConfigurationError("GRPCAPIServerListenAddresses must be specified")
		}
		if c.GRPCAPIServerTLSConfig.Enabled {
			if c.GRPCAPIServerTLSConfig.CertPath == "" {
				return errors.NewInvalidConfigurationError("GRPCAPIServerTLSConfig.CertPath must be specified for GRPC API server")
			}
			if c.GRPCAPIServerTLSConfig.KeyPath == "" {
				return errors.NewInvalidConfigurationError("GRPCAPIServerTLSConfig.KeyPath must be specified for GRPC API server")
			}
			if c.GRPCAPIServerTLSConfig.ClientAuth != "" && c.GRPCAPIServerTLSConfig.ClientCertsPath == "" {
				return errors.NewInvalidConfigurationError("GRPCAPIServerTLSConfig.ClientCertsPath must be provided if client auth is enabled")
			}
		}
	}
	if !c.TestServer {
		if c.NodeID >= len(c.RaftListenAddresses) {
			return errors.NewInvalidConfigurationError("NodeID must be in the range 0 (inclusive) to len(RaftListenAddresses) (exclusive)")
		}
		if c.DataDir == "" {
			return errors.NewInvalidConfigurationError("DataDir must be specified")
		}
		if c.ReplicationFactor < 3 {
			return errors.NewInvalidConfigurationError("ReplicationFactor must be >= 3")
		}
		if len(c.RaftListenAddresses) < c.ReplicationFactor {
			return errors.NewInvalidConfigurationError("Number of RaftListenAddresses must be >= ReplicationFactor")
		}
		if len(c.RemotingListenAddresses) != len(c.RaftListenAddresses) {
			return errors.NewInvalidConfigurationError("Number of RaftListenAddresses must be same as number of RemotingListenAddresses")
		}
		if c.GRPCAPIServerEnabled && len(c.GRPCAPIServerListenAddresses) != len(c.RaftListenAddresses) {
			return errors.NewInvalidConfigurationError("Number of RaftListenAddresses must be same as number of GRPCAPIServerListenAddresses")
		}
		if c.HTTPAPIServerEnabled && len(c.HTTPAPIServerListenAddresses) != len(c.RaftListenAddresses) {
			return errors.NewInvalidConfigurationError("Number of RaftListenAddresses must be same as number of HTTPAPIServerListenAddresses")
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
	if c.LifecycleEndpointEnabled {
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
	if c.RaftCallTimeout < 1*time.Second {
		return errors.NewInvalidConfigurationError("RaftCallTimeout must be >= 1 second")
	}
	if c.MaxProcessBatchSize < 1 {
		return errors.NewInvalidConfigurationError("MaxProcessBatchSize must be > 0")
	}
	if c.MaxForwardWriteBatchSize < 1 {
		return errors.NewInvalidConfigurationError("MaxForwardWriteBatchSize must be > 0")
	}
	if c.IntraClusterTLSConfig.Enabled {
		if c.IntraClusterTLSConfig.CertPath == "" {
			return errors.NewInvalidConfigurationError("IntraClusterTLSConfig.CertPath must be specified if intra cluster TLS is enabled")
		}
		if c.IntraClusterTLSConfig.KeyPath == "" {
			return errors.NewInvalidConfigurationError("IntraClusterTLSConfig.KeyPath must be specified if intra cluster TLS is enabled")
		}
		if c.IntraClusterTLSConfig.ClientCertsPath == "" {
			return errors.NewInvalidConfigurationError("IntraClusterTLSConfig.ClientCertsPath must be provided if intra cluster TLS is enabled")
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
		RaftCallTimeout:            DefaultRaftCallTimeout,
		AggregationCacheSizeRows:   DefaultAggregationCacheSizeRows,
		MaxProcessBatchSize:        DefaultMaxProcessBatchSize,
		MaxForwardWriteBatchSize:   DefaultMaxForwardWriteBatchSize,
		MaxTableReaperBatchSize:    DefaultMaxTableReaperBatchSize,
	}
}

func NewTestConfig(fakeKafkaID int64) *Config {
	return &Config{
		RaftRTTMs:                DefaultRaftRTTMs,
		RaftHeartbeatRTT:         DefaultRaftHeartbeatRTT,
		RaftElectionRTT:          DefaultRaftElectionRTT,
		RaftCallTimeout:          DefaultRaftCallTimeout,
		AggregationCacheSizeRows: DefaultAggregationCacheSizeRows,
		MaxProcessBatchSize:      DefaultMaxProcessBatchSize,
		MaxForwardWriteBatchSize: DefaultMaxForwardWriteBatchSize,
		MaxTableReaperBatchSize:  DefaultMaxTableReaperBatchSize,
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
