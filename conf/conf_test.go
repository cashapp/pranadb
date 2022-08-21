package conf

import (
	"fmt"
	"github.com/squareup/pranadb/errors"
	"github.com/stretchr/testify/require"
	"testing"
)

type configPair struct {
	errMsg string
	conf   Config
}

func invalidNodeIDConf() Config {
	cnf := confAllFields
	cnf.NodeID = -1
	return cnf
}

func invalidNumShardsConf() Config {
	cnf := confAllFields
	cnf.NumShards = 0
	return cnf
}

func invalidDatadirConf() Config {
	cnf := confAllFields
	cnf.DataDir = ""
	return cnf
}

func missingKafkaBrokersConf() Config {
	cnf := confAllFields
	cnf.KafkaBrokers = nil
	return cnf
}

func invalidBrokerClientTypeConf() Config {
	cnf := confAllFields
	cnfsCopy := make(map[string]BrokerConfig)
	for k, v := range cnf.KafkaBrokers {
		bc := v
		bc.ClientType = BrokerClientTypeUnknown
		cnfsCopy[k] = bc
	}
	cnf.KafkaBrokers = cnfsCopy
	return cnf
}

func invalidGRPCAPIServerListenAddress() Config {
	cnf := confAllFields
	cnf.EnableGRPCAPIServer = true
	cnf.GRPCAPIServerListenAddresses = nil
	return cnf
}

func invalidHTTPAPIServerListenAddress() Config {
	cnf := confAllFields
	cnf.EnableHTTPAPIServer = true
	cnf.HTTPAPIServerListenAddresses = nil
	return cnf
}

func invalidReplicationFactorConfig() Config {
	cnf := confAllFields
	cnf.ReplicationFactor = 2
	return cnf
}

func invalidRaftAddressesConfig() Config {
	cnf := confAllFields
	cnf.RaftAddresses = cnf.RaftAddresses[1:]
	return cnf
}

func raftAndNotifListenerAddressedDifferentLengthConfig() Config {
	cnf := confAllFields
	cnf.NotifListenAddresses = append(cnf.NotifListenAddresses, "someotheraddresss")
	return cnf
}

func raftAndGRPCAPIServerListenerAddressedDifferentLengthConfig() Config {
	cnf := confAllFields
	cnf.EnableGRPCAPIServer = true
	cnf.GRPCAPIServerListenAddresses = append(cnf.GRPCAPIServerListenAddresses, "someotheraddresss")
	return cnf
}

func raftAndHTTPAPIServerListenerAddressedDifferentLengthConfig() Config {
	cnf := confAllFields
	cnf.EnableHTTPAPIServer = true
	cnf.HTTPAPIServerListenAddresses = append(cnf.HTTPAPIServerListenAddresses, "someotheraddresss")
	return cnf
}

func httpAPIServerTLSKeyPathNotSpecifiedConfig() Config {
	cnf := confAllFields
	cnf.EnableHTTPAPIServer = true
	cnf.HTTPAPIServerTLSConfig.KeyPath = ""
	return cnf
}

func httpAPIServerTLSCertPathNotSpecifiedConfig() Config {
	cnf := confAllFields
	cnf.EnableHTTPAPIServer = true
	cnf.HTTPAPIServerTLSConfig.CertPath = ""
	return cnf
}

func invalidDataSnapshotEntries() Config {
	cnf := confAllFields
	cnf.DataSnapshotEntries = 9
	return cnf
}

func invalidDataCompactionOverhead() Config {
	cnf := confAllFields
	cnf.DataCompactionOverhead = 4
	return cnf
}

func invalidSequenceSnapshotEntries() Config {
	cnf := confAllFields
	cnf.SequenceSnapshotEntries = 9
	return cnf
}

func invalidSequenceCompactionOverhead() Config {
	cnf := confAllFields
	cnf.SequenceCompactionOverhead = 4
	return cnf
}

func invalidLocksSnapshotEntries() Config {
	cnf := confAllFields
	cnf.LocksSnapshotEntries = 9
	return cnf
}

func invalidLocksCompactionOverhead() Config {
	cnf := confAllFields
	cnf.LocksCompactionOverhead = 4
	return cnf
}

func dataCompactionGreaterThanDataSnapshotEntries() Config {
	cnf := confAllFields
	cnf.DataSnapshotEntries = 10
	cnf.DataCompactionOverhead = 11
	return cnf
}

func sequenceCompactionGreaterThanDataSnapshotEntries() Config {
	cnf := confAllFields
	cnf.SequenceSnapshotEntries = 10
	cnf.SequenceCompactionOverhead = 11
	return cnf
}

func locksCompactionGreaterThanDataSnapshotEntries() Config {
	cnf := confAllFields
	cnf.LocksSnapshotEntries = 10
	cnf.LocksCompactionOverhead = 11
	return cnf
}

func NodeIDOutOfRangeConf() Config {
	cnf := confAllFields
	cnf.NodeID = len(cnf.RaftAddresses)
	return cnf
}

const (
	lifeCycleListenAddress = "localhost:8765"
	startupEndpointPath    = "/started"
	liveEndpointPath       = "/liveness"
	readyEndpointPath      = "/readiness"
)

func invalidLifecycleListenAddress() Config {
	cnf := confAllFields
	cnf.EnableLifecycleEndpoint = true
	cnf.LifeCycleListenAddress = ""
	cnf.StartupEndpointPath = startupEndpointPath
	cnf.LiveEndpointPath = liveEndpointPath
	cnf.ReadyEndpointPath = readyEndpointPath
	return cnf
}

func invalidStartupEndpointPath() Config {
	cnf := confAllFields
	cnf.EnableLifecycleEndpoint = true
	cnf.LifeCycleListenAddress = lifeCycleListenAddress
	cnf.StartupEndpointPath = ""
	cnf.LiveEndpointPath = liveEndpointPath
	cnf.ReadyEndpointPath = readyEndpointPath
	return cnf
}

func invalidLiveEndpointPath() Config {
	cnf := confAllFields
	cnf.EnableLifecycleEndpoint = true
	cnf.LifeCycleListenAddress = lifeCycleListenAddress
	cnf.StartupEndpointPath = startupEndpointPath
	cnf.LiveEndpointPath = ""
	cnf.ReadyEndpointPath = readyEndpointPath
	return cnf
}

func invalidReadyEndpointPath() Config {
	cnf := confAllFields
	cnf.EnableLifecycleEndpoint = true
	cnf.LifeCycleListenAddress = lifeCycleListenAddress
	cnf.StartupEndpointPath = startupEndpointPath
	cnf.LiveEndpointPath = liveEndpointPath
	cnf.ReadyEndpointPath = ""
	return cnf
}

func invalidRaftRTTMsZero() Config {
	cnf := confAllFields
	cnf.RaftRTTMs = 0
	return cnf
}

func invalidRaftRTTMsNegative() Config {
	cnf := confAllFields
	cnf.RaftRTTMs = -1
	return cnf
}

func invalidRaftHeartbeatRTTZero() Config {
	cnf := confAllFields
	cnf.RaftHeartbeatRTT = 0
	return cnf
}

func invalidRaftHeartbeatRTTNegative() Config {
	cnf := confAllFields
	cnf.RaftHeartbeatRTT = -1
	return cnf
}

func invalidRaftElectionRTTZero() Config {
	cnf := confAllFields
	cnf.RaftElectionRTT = 0
	return cnf
}

func invalidRaftElectionRTTNegative() Config {
	cnf := confAllFields
	cnf.RaftElectionRTT = -1
	return cnf
}

func invalidRaftElectionRTTTooSmall() Config {
	cnf := confAllFields
	cnf.RaftElectionRTT = 1 + cnf.RaftHeartbeatRTT
	return cnf
}

func invalidMaxProcessorBatchSize() Config {
	cnf := confAllFields
	cnf.MaxProcessBatchSize = 0
	return cnf
}

func invalidMaxForwardWriteBatchSize() Config {
	cnf := confAllFields
	cnf.MaxForwardWriteBatchSize = 0
	return cnf
}

var invalidConfigs = []configPair{
	{"PDB3000 - Invalid configuration: NodeID must be >= 0", invalidNodeIDConf()},
	{"PDB3000 - Invalid configuration: NumShards must be >= 1", invalidNumShardsConf()},
	{"PDB3000 - Invalid configuration: DataDir must be specified", invalidDatadirConf()},
	{"PDB3000 - Invalid configuration: KafkaBrokers must be specified", missingKafkaBrokersConf()},
	{"PDB3000 - Invalid configuration: KafkaBroker testbroker, invalid ClientType, must be 1 or 2", invalidBrokerClientTypeConf()},
	{"PDB3000 - Invalid configuration: GRPCAPIServerListenAddresses must be specified", invalidGRPCAPIServerListenAddress()},
	{"PDB3000 - Invalid configuration: HTTPAPIServerListenAddresses must be specified", invalidHTTPAPIServerListenAddress()},
	{"PDB3000 - Invalid configuration: NodeID must be in the range 0 (inclusive) to len(RaftAddresses) (exclusive)", NodeIDOutOfRangeConf()},
	{"PDB3000 - Invalid configuration: ReplicationFactor must be >= 3", invalidReplicationFactorConfig()},
	{"PDB3000 - Invalid configuration: Number of RaftAddresses must be >= ReplicationFactor", invalidRaftAddressesConfig()},
	{"PDB3000 - Invalid configuration: Number of RaftAddresses must be same as number of NotifListenerAddresses", raftAndNotifListenerAddressedDifferentLengthConfig()},
	{"PDB3000 - Invalid configuration: Number of RaftAddresses must be same as number of GRPCAPIServerListenAddresses", raftAndGRPCAPIServerListenerAddressedDifferentLengthConfig()},
	{"PDB3000 - Invalid configuration: Number of RaftAddresses must be same as number of HTTPAPIServerListenAddresses", raftAndHTTPAPIServerListenerAddressedDifferentLengthConfig()},
	{"PDB3000 - Invalid configuration: DataSnapshotEntries must be >= 10", invalidDataSnapshotEntries()},
	{"PDB3000 - Invalid configuration: DataCompactionOverhead must be >= 5", invalidDataCompactionOverhead()},
	{"PDB3000 - Invalid configuration: SequenceSnapshotEntries must be >= 10", invalidSequenceSnapshotEntries()},
	{"PDB3000 - Invalid configuration: SequenceCompactionOverhead must be >= 5", invalidSequenceCompactionOverhead()},
	{"PDB3000 - Invalid configuration: LocksSnapshotEntries must be >= 10", invalidLocksSnapshotEntries()},
	{"PDB3000 - Invalid configuration: LocksCompactionOverhead must be >= 5", invalidLocksCompactionOverhead()},
	{"PDB3000 - Invalid configuration: DataSnapshotEntries must be >= DataCompactionOverhead", dataCompactionGreaterThanDataSnapshotEntries()},
	{"PDB3000 - Invalid configuration: SequenceSnapshotEntries must be >= SequenceCompactionOverhead", sequenceCompactionGreaterThanDataSnapshotEntries()},
	{"PDB3000 - Invalid configuration: LocksSnapshotEntries must be >= LocksCompactionOverhead", locksCompactionGreaterThanDataSnapshotEntries()},
	{"PDB3000 - Invalid configuration: LifeCycleListenAddress must be specified", invalidLifecycleListenAddress()},
	{"PDB3000 - Invalid configuration: StartupEndpointPath must be specified", invalidStartupEndpointPath()},
	{"PDB3000 - Invalid configuration: LiveEndpointPath must be specified", invalidLiveEndpointPath()},
	{"PDB3000 - Invalid configuration: ReadyEndpointPath must be specified", invalidReadyEndpointPath()},
	{"PDB3000 - Invalid configuration: RaftRTTMs must be > 0", invalidRaftRTTMsZero()},
	{"PDB3000 - Invalid configuration: RaftRTTMs must be > 0", invalidRaftRTTMsNegative()},
	{"PDB3000 - Invalid configuration: RaftHeartbeatRTT must be > 0", invalidRaftHeartbeatRTTZero()},
	{"PDB3000 - Invalid configuration: RaftHeartbeatRTT must be > 0", invalidRaftHeartbeatRTTNegative()},
	{"PDB3000 - Invalid configuration: RaftElectionRTT must be > 0", invalidRaftElectionRTTZero()},
	{"PDB3000 - Invalid configuration: RaftElectionRTT must be > 0", invalidRaftElectionRTTNegative()},
	{"PDB3000 - Invalid configuration: RaftElectionRTT must be > 2 * RaftHeartbeatRTT", invalidRaftElectionRTTTooSmall()},
	{"PDB3000 - Invalid configuration: MaxProcessBatchSize must be > 0", invalidMaxProcessorBatchSize()},
	{"PDB3000 - Invalid configuration: MaxForwardWriteBatchSize must be > 0", invalidMaxForwardWriteBatchSize()},
	{"PDB3000 - Invalid configuration: HTTPAPIServerTLSConfig.KeyPath must be specified for HTTP API server", httpAPIServerTLSKeyPathNotSpecifiedConfig()},
	{"PDB3000 - Invalid configuration: HTTPAPIServerTLSConfig.CertPath must be specified for HTTP API server", httpAPIServerTLSCertPathNotSpecifiedConfig()},
}

func TestValidate(t *testing.T) {
	for _, cp := range invalidConfigs {
		err := cp.conf.Validate()
		require.Error(t, err)
		pe, ok := errors.Cause(err).(errors.PranaError)
		require.True(t, ok)
		require.Equal(t, errors.InvalidConfiguration, int(pe.Code))
		require.Equal(t, cp.errMsg, pe.Msg)
	}
}

var confAllFields = Config{
	NodeID:               0,
	ClusterID:            12345,
	RaftAddresses:        []string{"addr1", "addr2", "addr3"},
	NotifListenAddresses: []string{"addr4", "addr5", "addr6"},
	NumShards:            50,
	ReplicationFactor:    3,
	DataDir:              "foo/bar/baz",
	TestServer:           false,
	KafkaBrokers: BrokerConfigs{
		"testbroker": BrokerConfig{
			ClientType: BrokerClientFake,
			Properties: map[string]string{
				"fakeKafkaID": fmt.Sprintf("%d", 1),
			},
		},
	},
	DataSnapshotEntries:          1001,
	DataCompactionOverhead:       501,
	SequenceSnapshotEntries:      2001,
	SequenceCompactionOverhead:   1001,
	LocksSnapshotEntries:         101,
	LocksCompactionOverhead:      51,
	EnableGRPCAPIServer:          true,
	GRPCAPIServerListenAddresses: []string{"addr7", "addr8", "addr9"},
	EnableHTTPAPIServer:          true,
	HTTPAPIServerListenAddresses: []string{"addr10", "addr11", "addr12"},
	HTTPAPIServerTLSConfig: TLSConfig{
		KeyPath:  "http_key_path",
		CertPath: "http_cert_path",
	},
	RaftRTTMs:                100,
	RaftHeartbeatRTT:         10,
	RaftElectionRTT:          100,
	MaxProcessBatchSize:      DefaultMaxForwardWriteBatchSize,
	MaxForwardWriteBatchSize: DefaultMaxForwardWriteBatchSize,
}
