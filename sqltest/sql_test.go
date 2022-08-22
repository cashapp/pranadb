//go:build !largecluster
// +build !largecluster

package sqltest

import (
	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/internal/testcerts"
	"io/ioutil"
	"os"
	"testing"
)

func TestSQLClusteredFiveNodes(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: skipped")
	}

	log.Info("Running TestSQLClusteredFiveNodes")
	testSQL(t, false, 5, 3, false, tlsKeysInfo)
}

//func TestSQLFakeCluster(t *testing.T) {
//	log.Debug("Running TestSQLFakeCluster")
//	testSQL(t, true, 1, 0, false, tlsKeysInfo)
//}
//
//func TestSQLFakeClusterUsingHTTPAPI(t *testing.T) {
//	log.Debug("Running TestSQLFakeClusterUsingHTTPAPI")
//	testSQL(t, true, 1, 0, true, tlsKeysInfo)
//}
//
//func TestSQLClusteredThreeNodes(t *testing.T) {
//	if testing.Short() {
//		t.Skip("-short: skipped")
//	}
//	log.Info("Running TestSQLClusteredThreeNodes")
//	testSQL(t, false, 3, 3, false, tlsKeysInfo)
//}

var tlsKeysInfo *TLSKeysInfo

func TestMain(m *testing.M) {
	// Set up all the certs we need here. Creating certs is slow, we don't want to do it on each test

	log.Info("creating certificates required for tests")

	tmpDir, err := ioutil.TempDir("", "cli_test")
	if err != nil {
		log.Fatalf("failed to create tmp dir %v", err)
	}

	serverCertPath, serverKeyPath, err := testcerts.CreateCertKeyPairToTmpFile(tmpDir, nil, "acme badgers ltd.")
	if err != nil {
		log.Fatalf("failed to cert key pair %v", err)
	}
	clientCertPath, clientKeyPath, err := testcerts.CreateCertKeyPairToTmpFile(tmpDir, nil, "acme squirrels ltd.")
	if err != nil {
		log.Fatalf("failed to cert key pair %v", err)
	}

	tlsKeysInfo = &TLSKeysInfo{
		ServerCertPath: serverCertPath,
		ServerKeyPath:  serverKeyPath,
		ClientCertPath: clientCertPath,
		ClientKeyPath:  clientKeyPath,
	}

	defer func() {
		defer func() {
			err := os.RemoveAll(tmpDir)
			if err != nil {
				log.Fatalf("failed to remove test tmpdir %v", err)
			}
		}()
	}()
	m.Run()
}
