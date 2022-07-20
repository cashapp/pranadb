package main

import (
	"fmt"
	"io/fs"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/squareup/pranadb/conf"
	"github.com/stretchr/testify/require"
)

func TestParseConfigWithComments(t *testing.T) {
	hcl, err := ioutil.ReadFile("testdata/config.hcl")
	require.NoError(t, err)
	cnfExpected := createConfigWithAllFields()
	cnfExpected.NodeID = 2
	testRunner(t, hcl, cnfExpected, 2)
}

func testRunner(t *testing.T, b []byte, cnf conf.Config, nodeID int) {
	t.Helper()
	dataDir, err := ioutil.TempDir("", "runner-test")
	require.NoError(t, err)
	defer removeDataDir(dataDir)

	fName := filepath.Join(dataDir, "json1.conf")
	err = ioutil.WriteFile(fName, b, fs.ModePerm)
	require.NoError(t, err)

	r := &runner{}
	args := []string{"--config", fName, "--node-id", fmt.Sprintf("%d", nodeID)}
	require.NoError(t, r.run(args, false))

	actualConfig := r.getServer().GetConfig()
	require.Equal(t, cnf, actualConfig)
}

func removeDataDir(dataDir string) {
	if err := os.RemoveAll(dataDir); err != nil {
		log.Errorf("failed to remove datadir %v", err)
	}
}

func createConfigWithAllFields() conf.Config {
	return conf.Config{
		ClusterID:            6112451081796031488,
		RaftAddresses:        []string{"addr1", "addr2", "addr3"},
		NotifListenAddresses: []string{"addr4", "addr5", "addr6"},
		NumShards:            50,
		ReplicationFactor:    3,
		DataDir:              "foo/bar/baz",
		TestServer:           false,
		KafkaBrokers: conf.BrokerConfigs{
			"testbroker": conf.BrokerConfig{
				ClientType: conf.BrokerClientFake,
				Properties: map[string]string{
					"fakeKafkaID": "1",
				},
			},
			"testbroker2": conf.BrokerConfig{
				ClientType: conf.BrokerClientDefault,
				Properties: map[string]string{
					"fakeKafkaID": "23",
					"otherProp":   "xyz",
				},
			},
		},
		DataSnapshotEntries:        1001,
		DataCompactionOverhead:     501,
		SequenceSnapshotEntries:    2001,
		SequenceCompactionOverhead: 1001,
		LocksSnapshotEntries:       101,
		LocksCompactionOverhead:    51,
		RemotingHeartbeatInterval:  76 * time.Second,
		RemotingHeartbeatTimeout:   5 * time.Second,
		EnableAPIServer:            true,
		APIServerListenAddresses:   []string{"addr7", "addr8", "addr9"},
		MetricsBind:                "localhost:9102",
		EnableMetrics:              false,
		RaftRTTMs:                  100,
		RaftElectionRTT:            300,
		RaftHeartbeatRTT:           30,
		ProcessorMaxLag:            6500 * time.Millisecond,
		FillMaxLag:                 12000 * time.Millisecond,
		SourceLagTimeout:           65000 * time.Millisecond,
	}
}
