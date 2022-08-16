package client

import (
	"github.com/squareup/pranadb/conf"
	"github.com/squareup/pranadb/server"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSessionTimeout(t *testing.T) {
	cfg := conf.NewTestConfig(1)
	cfg.EnableGRPCAPIServer = true
	serverAddress := "localhost:6584"
	cfg.GRPCAPIServerListenAddresses = []string{serverAddress}
	s, err := server.NewServer(*cfg)
	require.NoError(t, err)
	err = s.Start()
	require.NoError(t, err)
	defer func() {
		err = s.Stop()
		require.NoError(t, err)
	}()

	cli := NewClientUsingGRPC(serverAddress)
	err = cli.Start()
	require.NoError(t, err)
	defer func() {
		err = cli.Stop()
		require.NoError(t, err)
	}()
	ch, err := cli.ExecuteStatement("use sys", nil, nil)
	require.NoError(t, err)
	for range ch {
	}
	ch, err = cli.ExecuteStatement("select * from sys.tables", nil, nil)
	require.NoError(t, err)
	for range ch {
	}
}
