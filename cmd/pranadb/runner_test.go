package main

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"io/fs"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

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
		DataSnapshotEntries:          1001,
		DataCompactionOverhead:       501,
		SequenceSnapshotEntries:      2001,
		SequenceCompactionOverhead:   1001,
		LocksSnapshotEntries:         101,
		LocksCompactionOverhead:      51,
		EnableGRPCAPIServer:          true,
		GRPCAPIServerListenAddresses: []string{"addr7", "addr8", "addr9"},
		GRPCAPIServerTLSConfig: conf.TLSConfig{
			EnableTLS:       true,
			KeyPath:         "grpc-key-path",
			CertPath:        "grpc-cert-path",
			ClientCertsPath: "grpc-client-certs-path",
			ClientAuth:      "require-and-verify-client-cert",
		},
		EnableHTTPAPIServer:          true,
		HTTPAPIServerListenAddresses: []string{"addr7-1", "addr8-1", "addr9-1"},
		HTTPAPIServerTLSConfig: conf.TLSConfig{
			EnableTLS:       true,
			KeyPath:         "http-key-path",
			CertPath:        "http-cert-path",
			ClientCertsPath: "http-client-certs-path",
			ClientAuth:      "require-and-verify-client-cert",
		},
		MetricsBind:              "localhost:9102",
		EnableMetrics:            false,
		RaftRTTMs:                100,
		RaftElectionRTT:          300,
		RaftHeartbeatRTT:         30,
		DisableFsync:             true,
		AggregationCacheSizeRows: 1234,
		MaxProcessBatchSize:      777,
		MaxForwardWriteBatchSize: 888,

		DDProfilerTypes:           "HEAP,CPU",
		DDProfilerServiceName:     "my-service",
		DDProfilerEnvironmentName: "playing",
		DDProfilerPort:            1324,
		DDProfilerVersionName:     "2.3",
		DDProfilerHostEnvVarName:  "FOO_IP",
	}
}
