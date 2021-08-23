package conf

import (
	"fmt"
	"github.com/squareup/pranadb/perrors"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
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

func invalidClusterIDConf() Config {
	cnf := confAllFields
	cnf.ClusterID = -1
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

func invalidNotifierHeartbeatInterval() Config {
	cnf := confAllFields
	cnf.NotifierHeartbeatInterval = time.Second - 1
	return cnf
}

func invalidAPIServerListenAddress() Config {
	cnf := confAllFields
	cnf.EnableAPIServer = true
	cnf.APIServerListenAddress = ""
	return cnf
}

func invalidAPIServerSessionTimeout() Config {
	cnf := confAllFields
	cnf.EnableAPIServer = true
	cnf.APIServerSessionTimeout = 5*time.Second - 1
	return cnf
}

func invalidAPIServerSessionCheckInterval() Config {
	cnf := confAllFields
	cnf.EnableAPIServer = true
	cnf.APIServerSessionCheckInterval = 1*time.Second - 1
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

var invalidConfigs = []configPair{
	{"PDB0004 - Invalid configuration: NodeID must be >= 0", invalidNodeIDConf()},
	{"PDB0004 - Invalid configuration: ClusterID must be >= 0", invalidClusterIDConf()},
	{"PDB0004 - Invalid configuration: NumShards must be >= 1", invalidNumShardsConf()},
	{"PDB0004 - Invalid configuration: DataDir must be specified", invalidDatadirConf()},
	{"PDB0004 - Invalid configuration: KafkaBrokers must be specified", missingKafkaBrokersConf()},
	{"PDB0004 - Invalid configuration: KafkaBroker testbroker, invalid ClientType, must be 1 or 2", invalidBrokerClientTypeConf()},
	{"PDB0004 - Invalid configuration: NotifierHeartbeatInterval must be >= 1000000000", invalidNotifierHeartbeatInterval()},
	{"PDB0004 - Invalid configuration: APIServerListenAddress must be specified", invalidAPIServerListenAddress()},
	{"PDB0004 - Invalid configuration: APIServerSessionTimeout must be >= 5000000000", invalidAPIServerSessionTimeout()},
	{"PDB0004 - Invalid configuration: APIServerSessionCheckInterval must be >= 1000000000", invalidAPIServerSessionCheckInterval()},
	{"PDB0004 - Invalid configuration: ReplicationFactor must be >= 3", invalidReplicationFactorConfig()},
	{"PDB0004 - Invalid configuration: Number of RaftAddresses must be >= ReplicationFactor", invalidRaftAddressesConfig()},
	{"PDB0004 - Invalid configuration: Number of RaftAddresses must be same as number of NotifListenerAddresses", raftAndNotifListenerAddressedDifferentLengthConfig()},
	{"PDB0004 - Invalid configuration: DataSnapshotEntries must be >= 10", invalidDataSnapshotEntries()},
	{"PDB0004 - Invalid configuration: DataCompactionOverhead must be >= 5", invalidDataCompactionOverhead()},
	{"PDB0004 - Invalid configuration: SequenceSnapshotEntries must be >= 10", invalidSequenceSnapshotEntries()},
	{"PDB0004 - Invalid configuration: SequenceCompactionOverhead must be >= 5", invalidSequenceCompactionOverhead()},
	{"PDB0004 - Invalid configuration: LocksSnapshotEntries must be >= 10", invalidLocksSnapshotEntries()},
	{"PDB0004 - Invalid configuration: LocksCompactionOverhead must be >= 5", invalidLocksCompactionOverhead()},
}

func TestValidate(t *testing.T) {
	for _, cp := range invalidConfigs {
		err := cp.conf.Validate()
		require.Error(t, err)
		pe, ok := err.(perrors.PranaError)
		require.True(t, ok)
		require.Equal(t, perrors.InvalidConfiguration, int(pe.Code))
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
	DataSnapshotEntries:           1001,
	DataCompactionOverhead:        501,
	SequenceSnapshotEntries:       2001,
	SequenceCompactionOverhead:    1001,
	LocksSnapshotEntries:          101,
	LocksCompactionOverhead:       51,
	Debug:                         true,
	NotifierHeartbeatInterval:     76 * time.Second,
	EnableAPIServer:               true,
	APIServerListenAddress:        "localhost:4567",
	APIServerSessionTimeout:       41 * time.Second,
	APIServerSessionCheckInterval: 6 * time.Second,
}
