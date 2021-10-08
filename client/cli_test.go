package client

import (
	"fmt"
	"testing"
	"time"

	"github.com/squareup/pranadb/common/commontest"
	"github.com/squareup/pranadb/conf"
	"github.com/squareup/pranadb/server"
	"github.com/stretchr/testify/require"
)

func TestSessionTimeout(t *testing.T) {
	cfg := conf.NewTestConfig(1)
	cfg.EnableAPIServer = true
	serverAddress := "localhost:6584"
	cfg.APIServerListenAddresses = []string{serverAddress}
	cfg.APIServerSessionCheckInterval = 100 * time.Millisecond
	cfg.APIServerSessionTimeout = 1 * time.Second
	s, err := server.NewServer(*cfg)
	require.NoError(t, err)
	err = s.Start()
	require.NoError(t, err)
	defer func() {
		err = s.Stop()
		require.NoError(t, err)
	}()

	cli := NewClient(serverAddress, 5*time.Second)
	err = cli.Start()
	require.NoError(t, err)
	defer func() {
		err = cli.Stop()
		require.NoError(t, err)
	}()
	sess, err := cli.CreateSession()
	require.NoError(t, err)
	require.NotNil(t, sess)
	ch, err := cli.ExecuteStatement(sess, "use sys")
	require.NoError(t, err)
	for range ch {
	}
	ch, err = cli.ExecuteStatement(sess, "select * from sys.tables")
	require.NoError(t, err)
	for range ch {
	}
	cli.disableHeartbeats()
	commontest.WaitUntil(t, func() (bool, error) {
		ch, err = cli.ExecuteStatement(sess, "select * from sys.tables")
		require.NoError(t, err)
		for line := range ch {
			if line == fmt.Sprintf("Failed to execute statement: PDB0003 - Unknown session ID %s", sess) {
				return true, nil
			}
		}
		return false, err
	})
}
