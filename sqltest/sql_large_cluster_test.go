//go:build largecluster
// +build largecluster

package sqltest

import (
	"testing"

	log "github.com/sirupsen/logrus"
)

// These tests are run in a separate CI run as they can take a longer time to run

func TestSQLClusteredFiveNodes(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: skipped")
	}

	log.Info("Running TestSQLClusteredFiveNodes")
	testSQL(t, false, 5, 3, false, false, tlsKeysInfo)
}

func TestSQLClusteredSevenNodesReplicationFive(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: skipped")
	}
	log.Info("Running TestSQLClusteredSevenNodesReplicationFive")
	testSQL(t, false, 7, 5, false, false, tlsKeysInfo)
}

func TestSQLClusteredSevenNodesReplicationThree(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: skipped")
	}
	log.Info("Running TestSQLClusteredSevenNodesReplicationFive")
	testSQL(t, false, 7, 3, false, false, tlsKeysInfo)
}
