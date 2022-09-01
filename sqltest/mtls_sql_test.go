//go:build mtlssqltest
// +build mtlssqltest

package sqltest

import (
	"testing"

	log "github.com/sirupsen/logrus"
)

func TestSQLClusteredThreeNodesTLS(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: skipped")
	}
	log.Info("Running TestSQLClusteredThreeNodesTLS")
	testSQL(t, false, 3, 3, false, true, tlsKeysInfo)
}
