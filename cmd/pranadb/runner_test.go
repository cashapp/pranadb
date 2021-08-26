package main

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/squareup/pranadb/conf"
	"github.com/stretchr/testify/require"
)

func TestRunnerConfigAllFieldsSpecified(t *testing.T) {
	t.Skip("Duration is marshaled as number but parser expects a string. Need a fix for Kong.")
	cnfExpected := createConfigWithAllFields()
	cnfExpected.NodeID = 2
	cnfToWrite := cnfExpected
	cnfToWrite.NodeID = 12345 // Node id is not taken from the config file
	b, err := json.MarshalIndent(cnfToWrite, " ", " ")
	require.NoError(t, err)
	fmt.Println(string(b))
	testRunner(t, b, cnfExpected, 2)
}

func TestParseConfigWithComments(t *testing.T) {
	jsonWithComments := `
	{
	  "cluster_id": 12345, // and this is the clusterid
/* These 
are the raft addresses
*/
	  "raft_addresses": [
	   "addr1",
	   "addr2",
	   "addr3"
	  ],
	  "notif_listen_addresses": [
	   "addr4",
	   "addr5",
	   "addr6"
	  ],
      // Numshards
	  "num_shards": 50,
	  "replication_factor": 3,
	  "data_dir": "foo/bar/baz",
	  "test_server": false,
	  "kafka_brokers": {
	   "testbroker": {
		"client_type": 1,
		"properties": {
		 "fakeKafkaID": "1"
		}
	   }
	  },
	  "data_snapshot_entries": 1001,
	  "data_compaction_overhead": 501,
	  "sequence_snapshot_entries": 2001,
	  "sequence_compaction_overhead": 1001,
	  "locks_snapshot_entries": 101,
	  "locks_compaction_overhead": 51,
	  "debug": true,
	  "notifier_heartbeat_interval": "76s",
	  "enable_api_server": true,
	  "api_server_listen_addresses": [
	    "addr7",
	    "addr8",
	    "addr9"
	  ],
	  "api_server_session_timeout": "41s",
	  "api_server_session_check_interval": "6s",
      "log_format": "json",
      "log_level": "info",
      "log_file": "-"
	 }
`
	cnfExpected := createConfigWithAllFields()
	cnfExpected.NodeID = 2
	testRunner(t, []byte(jsonWithComments), cnfExpected, 2)
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
		log.Printf("failed to remove datadir %v", err)
	}
}

func createConfigWithAllFields() conf.Config {
	return conf.Config{
		ClusterID:            12345,
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
		APIServerListenAddresses:      []string{"addr7", "addr8", "addr9"},
		APIServerSessionTimeout:       41 * time.Second,
		APIServerSessionCheckInterval: 6 * time.Second,
	}
}
