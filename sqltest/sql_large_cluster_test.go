//go:build largecluster
// +build largecluster

package sqltest

import (
	log "github.com/sirupsen/logrus"
	"testing"
)

// These tests are run in a separate CI run as they can take a longer time to run

func TestSQLClusteredSevenNodesReplicationFive(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: skipped")
	}
	log.Info("Running TestSQLClusteredSevenNodesReplicationFive")
	testSQL(t, false, 7, 5)
}
