package remoting

import (
	"fmt"
	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/protos/squareup/cash/pranadb/v1/clustermsgs"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestRPC(t *testing.T) {
	server := startServerWithListener(t, &echoListener{})
	defer stopServers(t, server)

	client := &Client{}

	msg := &clustermsgs.RemotingTestMessage{SomeField: "badgers"}
	r, err := client.SendRPC(msg, defaultServerAddress)
	require.NoError(t, err)
	resp, ok := r.(*clustermsgs.RemotingTestMessage)
	require.True(t, ok)
	require.Equal(t, "badgers", resp.SomeField)

	client.Stop()
}

func TestRPCInternalError(t *testing.T) {
	err := errors.New("spiders")
	testRPCError(t, err, func(t *testing.T, pranaError errors.PranaError) {
		t.Helper()
		require.Equal(t, int(errors.InternalError), int(pranaError.Code))
	})
}

func TestRPCPranaError(t *testing.T) {
	err := errors.NewPranaError(errors.UnknownSource, "unknown source foo")
	testRPCError(t, err, func(t *testing.T, pranaError errors.PranaError) {
		t.Helper()
		require.Equal(t, err.Code, pranaError.Code)
		require.Equal(t, err.Msg, pranaError.Msg)
	})
}

func testRPCError(t *testing.T, respErr error, checkFunc func(*testing.T, errors.PranaError)) {
	t.Helper()
	server := startServerWithListener(t, &returnErrListener{err: respErr})
	defer stopServers(t, server)

	client := &Client{}

	msg := &clustermsgs.RemotingTestMessage{SomeField: "badgers"}
	r, err := client.SendRPC(msg, defaultServerAddress)
	require.Error(t, err)
	require.Nil(t, r)
	perr, ok := err.(errors.PranaError)
	require.True(t, ok)

	checkFunc(t, perr)

	client.Stop()
}

func TestRPCConnectionError(t *testing.T) {
	server := startServerWithListener(t, &echoListener{})
	defer stopServers(t, server)

	client := &Client{}

	// Send request successfully
	msg := &clustermsgs.RemotingTestMessage{SomeField: "badgers"}
	r, err := client.SendRPC(msg, defaultServerAddress)
	require.NoError(t, err)
	resp, ok := r.(*clustermsgs.RemotingTestMessage)
	require.True(t, ok)
	require.Equal(t, "badgers", resp.SomeField)

	server.closeNetConns()

	r, err = client.SendRPC(msg, defaultServerAddress)
	require.Error(t, err)
	require.Nil(t, r)
	require.Equal(t, ErrConnectionClosed, err)

	client.Stop()
}

func TestBroadcast(t *testing.T) {
	numServers := 3
	var servers []*server
	var serverAddresses []string
	var listeners []*echoListener
	for i := 0; i < numServers; i++ {
		address := fmt.Sprintf("localhost:%d", 7888+i)
		serverAddresses = append(serverAddresses, address)
		listener := &echoListener{}
		listeners = append(listeners, listener)
		servers = append(servers, startServerWithListenerAndAddresss(t, listener, address))
	}
	defer stopServers(t, servers...)

	client := &Client{}

	msg := &clustermsgs.RemotingTestMessage{SomeField: "badgers"}
	err := client.Broadcast(msg, 1, serverAddresses...)
	require.NoError(t, err)
	for _, listener := range listeners {
		require.Equal(t, 1, listener.getCalledCount())
	}

	client.Stop()
}

func TestBroadcastErrorAllServers(t *testing.T) {
	numServers := 3
	var servers []*server
	var serverAddresses []string
	respErr := errors.New("spiders")
	listener := &returnErrListener{err: respErr}
	for i := 0; i < numServers; i++ {
		address := fmt.Sprintf("localhost:%d", 7888+i)
		serverAddresses = append(serverAddresses, address)
		servers = append(servers, startServerWithListenerAndAddresss(t, listener, address))
	}
	defer stopServers(t, servers...)

	client := &Client{}

	msg := &clustermsgs.RemotingTestMessage{SomeField: "badgers"}
	err := client.Broadcast(msg, 1, serverAddresses...)
	require.Error(t, err)
	perr, ok := err.(errors.PranaError)
	require.True(t, ok)
	require.Equal(t, int(errors.InternalError), int(perr.Code))

	client.Stop()
}

func TestBroadcastErrorOneServerInternalErrorNoDelays(t *testing.T) {
	err := errors.New("spiders")
	testBroadcastErrorOneServer(t, err, 0, 0, func(t *testing.T, pranaError errors.PranaError) {
		t.Helper()
		require.Equal(t, int(errors.InternalError), int(pranaError.Code))
	})
}

func TestBroadcastErrorOneServerPranaErrorNoDelays(t *testing.T) {
	err := errors.NewPranaError(errors.UnknownSource, "unknown source foo")
	testBroadcastErrorOneServer(t, err, 0, 0, func(t *testing.T, pranaError errors.PranaError) {
		t.Helper()
		require.Equal(t, err.Code, pranaError.Code)
		require.Equal(t, err.Msg, pranaError.Msg)
	})
}

func TestBroadcastErrorOneServerPranaErrorDelayError(t *testing.T) {
	err := errors.NewPranaError(errors.UnknownSource, "unknown source foo")
	testBroadcastErrorOneServer(t, err, 100*time.Millisecond, 0, func(t *testing.T, pranaError errors.PranaError) {
		t.Helper()
		require.Equal(t, err.Code, pranaError.Code)
		require.Equal(t, err.Msg, pranaError.Msg)
	})
}

func TestBroadcastErrorOneServerPranaErrorDelayNonError(t *testing.T) {
	err := errors.NewPranaError(errors.UnknownSource, "unknown source foo")
	testBroadcastErrorOneServer(t, err, 0, 100*time.Millisecond, func(t *testing.T, pranaError errors.PranaError) {
		t.Helper()
		require.Equal(t, err.Code, pranaError.Code)
		require.Equal(t, err.Msg, pranaError.Msg)
	})
}

func testBroadcastErrorOneServer(t *testing.T, respErr error, errDelay time.Duration, nonErrDelay time.Duration,
	checkFunc func(*testing.T, errors.PranaError)) {
	t.Helper()
	numServers := 3
	var servers []*server
	var serverAddresses []string

	for i := 0; i < numServers; i++ {
		address := fmt.Sprintf("localhost:%d", 7888+i)
		serverAddresses = append(serverAddresses, address)
		var listener ClusterMessageHandler
		// We put the error return on only one listener and we have optional delays on error and non error return
		// to check error and non error responses coming back in different orders
		if i == 1 {
			listener = &returnErrListener{err: respErr, delay: errDelay}
		} else {
			listener = &echoListener{delay: nonErrDelay}
		}
		servers = append(servers, startServerWithListenerAndAddresss(t, listener, address))
	}
	defer stopServers(t, servers...)

	client := &Client{}

	msg := &clustermsgs.RemotingTestMessage{SomeField: "badgers"}
	err := client.Broadcast(msg, 1, serverAddresses...)
	require.Error(t, err)
	perr, ok := err.(errors.PranaError)
	require.True(t, ok)

	checkFunc(t, perr)

	client.Stop()
}

func TestBroadcastNoServers(t *testing.T) {
	numServers := 3
	var serverAddresses []string
	for i := 0; i < numServers; i++ {
		address := fmt.Sprintf("localhost:%d", 7888+i)
		serverAddresses = append(serverAddresses, address)
	}

	client := &Client{}

	msg := &clustermsgs.RemotingTestMessage{SomeField: "badgers"}
	err := client.Broadcast(msg, 1, serverAddresses...)
	require.Error(t, err)
	require.Equal(t, ErrInsufficientServers, err)

	client.Stop()
}

func TestBroadcastNotEnoughServers(t *testing.T) {
	numServers := 3
	var serverAddresses []string
	for i := 0; i < numServers; i++ {
		address := fmt.Sprintf("localhost:%d", 7888+i)
		serverAddresses = append(serverAddresses, address)
	}

	// Just one server
	server := startServerWithListenerAndAddresss(t, &echoListener{}, serverAddresses[0])
	defer stopServers(t, server)

	client := &Client{}

	msg := &clustermsgs.RemotingTestMessage{SomeField: "badgers"}
	err := client.Broadcast(msg, 2, serverAddresses...)
	require.Error(t, err)
	require.Equal(t, ErrInsufficientServers, err)

	client.Stop()
}

func TestBroadcastJustEnoughServers(t *testing.T) {
	numServers := 3
	var serverAddresses []string
	for i := 0; i < numServers; i++ {
		address := fmt.Sprintf("localhost:%d", 7888+i)
		serverAddresses = append(serverAddresses, address)
	}

	// Just one server
	listener := &echoListener{}
	server := startServerWithListenerAndAddresss(t, listener, serverAddresses[0])
	defer stopServers(t, server)

	client := &Client{}

	msg := &clustermsgs.RemotingTestMessage{SomeField: "badgers"}
	err := client.Broadcast(msg, 1, serverAddresses...)
	require.NoError(t, err)
	require.Equal(t, 1, listener.getCalledCount())

	client.Stop()
}

func TestBroadcastConnectionError(t *testing.T) {
	numServers := 3
	var servers []*server
	var serverAddresses []string
	var listeners []*echoListener
	for i := 0; i < numServers; i++ {
		address := fmt.Sprintf("localhost:%d", 7888+i)
		serverAddresses = append(serverAddresses, address)
		listener := &echoListener{}
		listeners = append(listeners, listener)
		servers = append(servers, startServerWithListenerAndAddresss(t, listener, address))
	}
	defer stopServers(t, servers...)

	client := &Client{}

	// Send broadcast successfully
	msg := &clustermsgs.RemotingTestMessage{SomeField: "badgers"}
	err := client.Broadcast(msg, 1, serverAddresses...)
	require.NoError(t, err)
	for _, listener := range listeners {
		require.Equal(t, 1, listener.getCalledCount())
	}

	// Now kill all connections
	for _, server := range servers {
		server.closeNetConns()
	}

	// Try and send again
	err = client.Broadcast(msg, 1, serverAddresses...)
	require.Error(t, err)

	client.Stop()
}
