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
	cnfExpected := createConfigWithAllFields()
	cnfExpected.NodeID = 2
	cnfToWrite := cnfExpected
	cnfToWrite.NodeID = 12345 // Node id is not taken from the config file
	b, err := json.MarshalIndent(cnfToWrite, " ", " ")
	require.NoError(t, err)
	testRunner(t, b, cnfExpected, 2)
}

func TestParseConfigWithComments(t *testing.T) {
	jsonWithComments := `
	{
	  "ClusterID": 12345, // and this is the clusterid
/* These 
are the raft addresses
*/
	  "RaftAddresses": [
	   "addr1",
	   "addr2",
	   "addr3"
	  ],
	  "NotifListenAddresses": [
	   "addr4",
	   "addr5",
	   "addr6"
	  ],
      // Numshards
	  "NumShards": 50,
	  "ReplicationFactor": 3,
	  "DataDir": "foo/bar/baz",
	  "TestServer": false,
	  "KafkaBrokers": {
	   "testbroker": {
		"ClientType": 1,
		"Properties": {
		 "fakeKafkaID": "1"
		}
	   }
	  },
	  "DataSnapshotEntries": 1001,
	  "DataCompactionOverhead": 501,
	  "SequenceSnapshotEntries": 2001,
	  "SequenceCompactionOverhead": 1001,
	  "LocksSnapshotEntries": 101,
	  "LocksCompactionOverhead": 51,
	  "Debug": true,
	  "NotifierHeartbeatInterval": 76000000000,
	  "EnableAPIServer": true,
	  "APIServerListenAddresses": [
	   "addr7",
	   "addr8",
	   "addr9"
	  ],
	  "APIServerSessionTimeout": 41000000000,
	  "APIServerSessionCheckInterval": 6000000000,
      "LogFormat": "json",
      "LogLevel": "info",
      "LogFile": "-"
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
	args := []string{"-conf", fName, "-node", fmt.Sprintf("%d", nodeID)}
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
		LogFormat:                     "json",
		LogLevel:                      "info",
		LogFile:                       "-",
	}
}
