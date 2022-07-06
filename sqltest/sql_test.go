//go:build !largecluster
// +build !largecluster

package sqltest

import (
	log "github.com/sirupsen/logrus"
	"testing"
)

func TestSQLFakeCluster(t *testing.T) {
	log.Debug("Running TestSQLFakeCluster")
	testSQL(t, true, 1, 0)
}

func TestSQLClusteredFiveNodes(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: skipped")
	}

	log.Info("Running TestSQLClusteredFiveNodes")
	testSQL(t, false, 5, 3)
}

//func TestSQLClusteredThreeNodes(t *testing.T) {
//	if testing.Short() {
//		t.Skip("-short: skipped")
//	}
//	log.Info("Running TestSQLClusteredThreeNodes")
//	testSQL(t, false, 3, 3)
//}
