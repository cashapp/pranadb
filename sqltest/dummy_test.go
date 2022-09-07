package sqltest

import (
	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/internal/testcerts"
	"io/ioutil"
	"os"
	"testing"
)

// This test file doesn't contain any tests, just the test main and tlsKeysInfo
// We put it in another file because it needs to be in the package but not in the other test files which
// are controlled by build flags
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
	intraClusterCertPath, intraClusterKeyPath, err := testcerts.CreateCertKeyPairToTmpFile(tmpDir, nil, "acme badgers ltd.")
	if err != nil {
		log.Fatalf("failed to create cert key pair %v", err)
	}

	tlsKeysInfo = &TLSKeysInfo{
		ServerCertPath:       serverCertPath,
		ServerKeyPath:        serverKeyPath,
		ClientCertPath:       clientCertPath,
		ClientKeyPath:        clientKeyPath,
		IntraClusterCertPath: intraClusterCertPath,
		IntraClusterKeyPath:  intraClusterKeyPath,
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
