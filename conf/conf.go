package conf

import (
	"fmt"
	"time"

	"go.uber.org/zap"
)

const (
	DefaultDataSnapshotEntries        = 10000
	DefaultDataCompactionOverhead     = 2500
	DefaultSequenceSnapshotEntries    = 1000
	DefaultSequenceCompactionOverhead = 250
	DefaultLocksSnapshotEntries       = 1000
	DefaultLocksCompactionOverhead    = 250
	DefaultNotifierHeartbeatInterval  = 5 * time.Second
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
	NotifierHeartbeatInterval  time.Duration
	Logger                     *zap.Logger
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
		NotifierHeartbeatInterval:  DefaultNotifierHeartbeatInterval,
		Logger:                     NewLogger(),
	}
}

func NewTestConfig(fakeKafkaID int64, logger *zap.Logger) *Config {
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
		Logger: logger,
	}
}

func NewLogger() *zap.Logger {
	logger, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}
	return logger
}
