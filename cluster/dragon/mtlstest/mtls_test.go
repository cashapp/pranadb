package mtlstest

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/cluster/dragon/integration"
	"github.com/squareup/pranadb/conf"
	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/internal/testcerts"
	"github.com/stretchr/testify/require"
)

const (
	numShards         = 3
	replicationFactor = 3
)

type tearDownFunc func(t *testing.T)

func setupDragonCluster(t *testing.T) (*integration.TestCluster, tearDownFunc) {
	t.Helper()

	dataDir, err := ioutil.TempDir("", "dragon-mtls-test")
	require.NoError(t, err, "failed to create temp dir")
	tlsCertsDir, err := ioutil.TempDir("", "dragon-tls-certs")
	require.NoError(t, err, "failed to create tls certs dir")
	tlsConfig, err := createTLSConfig(tlsCertsDir)
	require.NoError(t, err)

	testCluster := integration.NewTestCluster(dataDir, numShards, replicationFactor, tlsConfig)
	err = testCluster.Start()
	require.NoError(t, err, "failed to start test dragon cluster")

	return testCluster, func(t *testing.T) {
		t.Helper()
		_ = testCluster.Stop()
		defer func() {
			// We want to do this even if stopDragonCluster fails hence the extra defer
			err := os.RemoveAll(dataDir)
			require.NoError(t, err, "failed to remove test dir")
		}()
		defer func() {
			// We want to do this even if stopDragonCluster fails hence the extra defer
			err := os.RemoveAll(tlsCertsDir)
			require.NoError(t, err, "failed to remove tls certs dir")
		}()
	}
}

func TestLocalPutGet(t *testing.T) {
	testCluster, tearDown := setupDragonCluster(t)
	defer tearDown(t)

	node, localShard := testCluster.GetLocalNodeAndShard()

	key := []byte("somekey")
	value := []byte("somevalue")

	kvPair := cluster.KVPair{
		Key:   key,
		Value: value,
	}

	wb := cluster.NewWriteBatch(localShard)
	wb.AddPut(kvPair.Key, kvPair.Value)

	err := node.WriteBatch(wb, false)
	require.NoError(t, err)

	res, err := node.LocalGet(key)
	require.NoError(t, err)
	require.NotNil(t, res)

	require.Equal(t, string(value), string(res))
}

func createTLSConfig(certsDir string) (conf.TLSConfig, error) {
	caCert, err := testcerts.CreateTestCACert("acme CA")
	config := conf.TLSConfig{}
	if err != nil {
		return config, errors.Wrapf(err, "failed to create ca cert")
	}
	caCertPath, err := testcerts.WritePemToTmpFile(certsDir, caCert.Pem.Bytes())
	if err != nil {
		return config, errors.Wrapf(err, "failed to write pem file")
	}

	certPath, keyPath, err := testcerts.CreateCertKeyPairToTmpFile(certsDir, caCert, "acme badgers ltd.")
	if err != nil {
		return config, errors.Wrapf(err, "failed to cert key pair")
	}
	config.Enabled = true
	config.ClientCertsPath = caCertPath
	config.CertPath = certPath
	config.KeyPath = keyPath
	return config, nil
}
