package conf

import "fmt"

const (
	DefaultDataSnapshotEntries        = 10000
	DefaultDataCompactionOverhead     = 2500
	DefaultSequenceSnapshotEntries    = 1000
	DefaultSequenceCompactionOverhead = 250
	DefaultLocksSnapshotEntries       = 1000
	DefaultLocksCompactionOverhead    = 250
)

type Config struct {
	NodeID                     int
	ClusterID                  int // All nodes in a Prana cluster must share the same ClusterID
	RaftAddresses              []string
	NotifListenAddresses       []string
	NumShards                  int
	ReplicationFactor          int
	DataDir                    string
	TestServer                 bool
	KafkaBrokers               BrokerConfigs
	DataSnapshotEntries        int
	DataCompactionOverhead     int
	SequenceSnapshotEntries    int
	SequenceCompactionOverhead int
	LocksSnapshotEntries       int
	LocksCompactionOverhead    int
	Debug                      bool
}

type BrokerConfigs map[string]BrokerConfig // Key is broker name which is referred to in the source descriptor

type BrokerClientType int

const (
	BrokerClientFake    BrokerClientType = 1
	BrokerClientDefault                  = 2
)

type BrokerConfig struct {
	ClientType BrokerClientType
	Properties map[string]string
}

func NewConfig() *Config {
	return &Config{
		DataSnapshotEntries:        DefaultDataSnapshotEntries,
		DataCompactionOverhead:     DefaultDataCompactionOverhead,
		SequenceSnapshotEntries:    DefaultSequenceSnapshotEntries,
		SequenceCompactionOverhead: DefaultSequenceCompactionOverhead,
		LocksSnapshotEntries:       DefaultLocksSnapshotEntries,
		LocksCompactionOverhead:    DefaultLocksCompactionOverhead,
	}
}

func NewTestConfig(fakeKafkaID int64) *Config {
	return &Config{
		NodeID:     0,
		NumShards:  10,
		TestServer: true,
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
