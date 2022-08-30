package remoting

import (
	"fmt"
	"io/ioutil"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/internal/testcerts"

	"github.com/squareup/pranadb/conf"
	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/protos/squareup/cash/pranadb/v1/clustermsgs"
	"github.com/stretchr/testify/require"
)

const defaultServerAddress = "localhost:7888"

func TestSendRequest(t *testing.T) {
	list := &echoListener{}
	server := startServerWithListener(t, list, conf.TLSConfig{})
	defer stopServers(t, server)

	conn, err := createConnection(defaultServerAddress, nil)
	require.NoError(t, err)

	msg := &clustermsgs.RemotingTestMessage{SomeField: "badgers"}
	rh := newRespHandler()
	err = conn.SendRequestAsync(msg, rh)
	require.NoError(t, err)
	r, err := rh.waitForResponse()
	require.NoError(t, err)
	resp, ok := r.(*clustermsgs.RemotingTestMessage)
	require.True(t, ok)
	require.Equal(t, "badgers", resp.SomeField)
	require.Equal(t, 1, list.getCalledCount())

	conn.Close()
}

func TestSendConcurrentRequests(t *testing.T) {
	list := &echoListener{}
	server := startServerWithListener(t, list, conf.TLSConfig{})
	defer stopServers(t, server)

	conn, err := createConnection(defaultServerAddress, nil)
	require.NoError(t, err)

	numRequests := 100
	var respHandlers []*testRespHandler
	for i := 0; i < numRequests; i++ {
		msg := &clustermsgs.RemotingTestMessage{SomeField: fmt.Sprintf("badgers-%d", i)}
		rh := newRespHandler()
		err = conn.SendRequestAsync(msg, rh)
		require.NoError(t, err)
		respHandlers = append(respHandlers, rh)
	}

	for i, rh := range respHandlers {
		r, err := rh.waitForResponse()
		require.NoError(t, err)
		resp, ok := r.(*clustermsgs.RemotingTestMessage)
		require.True(t, ok)
		require.Equal(t, fmt.Sprintf("badgers-%d", i), resp.SomeField)
	}
	require.Equal(t, numRequests, list.getCalledCount())

	conn.Close()
}

func TestSendRequestTLS(t *testing.T) {
	log.Info("creating certificates required for tests")

	tmpDir, err := ioutil.TempDir("", "cli_test")
	if err != nil {
		log.Fatalf("failed to create tmp dir %v", err)
	}
	defer func() {
		err := os.RemoveAll(tmpDir)
		if err != nil {
			log.Fatalf("failed to remove temp dir: %v", err)
		}
	}()
	serverCertPath, serverKeyPath, err := testcerts.CreateCertKeyPairToTmpFile(tmpDir, nil, "acme badgers ltd.")
	if err != nil {
		log.Errorf("failed to cert key pair %v", err)
	}
	tlsConf := conf.TLSConfig{
		Enabled:         true,
		KeyPath:         serverKeyPath,
		CertPath:        serverCertPath,
		ClientCertsPath: serverCertPath,
		ClientAuth:      "require-and-verify-client-cert",
	}

	list := &echoListener{}
	server := startServerWithListener(t, list, tlsConf)
	defer stopServers(t, server)

	clientTLSConfig, err := getClientTLSConfig(tlsConf)
	require.NoError(t, err)
	conn, err := createConnection(defaultServerAddress, clientTLSConfig)
	require.NoError(t, err)

	msg := &clustermsgs.RemotingTestMessage{SomeField: "badgers"}
	rh := newRespHandler()
	err = conn.SendRequestAsync(msg, rh)
	require.NoError(t, err)
	r, err := rh.waitForResponse()
	require.NoError(t, err)
	resp, ok := r.(*clustermsgs.RemotingTestMessage)
	require.True(t, ok)
	require.Equal(t, "badgers", resp.SomeField)
	require.Equal(t, 1, list.getCalledCount())

	conn.Close()
}

func TestResponseInternalError(t *testing.T) {

	// Non Prana errors will get logged and returned as internal error
	err := errors.New("spiders")

	testResponseError(t, err, func(t *testing.T, perr errors.PranaError) {
		t.Helper()
		require.Equal(t, int(errors.InternalError), int(perr.Code))
	})

}

func TestResponsePranaError(t *testing.T) {

	// Prana errors will get passed through
	err := errors.NewPranaError(errors.UnknownSource, "unknown source foo")

	testResponseError(t, err, func(t *testing.T, perr errors.PranaError) {
		t.Helper()
		require.Equal(t, int(errors.UnknownSource), int(perr.Code))
		require.Equal(t, err.Msg, perr.Msg)
	})
}

func testResponseError(t *testing.T, respErr error, checkFunc func(*testing.T, errors.PranaError)) {
	t.Helper()
	server := startServerWithListener(t, &returnErrListener{err: respErr}, conf.TLSConfig{})
	defer stopServers(t, server)

	conn, err := createConnection(defaultServerAddress, nil)
	require.NoError(t, err)

	msg := &clustermsgs.RemotingTestMessage{SomeField: "badgers"}
	rh := newRespHandler()
	err = conn.SendRequestAsync(msg, rh)
	require.NoError(t, err)
	r, err := rh.waitForResponse()
	require.Nil(t, r)
	require.Error(t, err)
	perr, ok := err.(errors.PranaError)
	require.True(t, ok)

	checkFunc(t, perr)

	conn.Close()
}

func TestConnectFailedNoServer(t *testing.T) {
	conn, err := createConnection("localhost:7888", nil)
	require.Error(t, err)
	require.Nil(t, conn)
}

func TestCloseConnectionFromServer(t *testing.T) {
	server := startServerWithListener(t, &echoListener{}, conf.TLSConfig{})
	defer stopServers(t, server)

	conn, err := createConnection(defaultServerAddress, nil)
	require.NoError(t, err)

	err = server.Stop()
	require.NoError(t, err)

	// Give a little time for the connection to be closed
	time.Sleep(1 * time.Second)

	handler := newRespHandler()
	err = conn.SendRequestAsync(&clustermsgs.RemotingTestMessage{SomeField: "badgers"}, handler)

	require.Error(t, err)
	require.Equal(t, ErrConnectionClosed, err)

	conn.Close()
}

func TestUseOfClosedConnection(t *testing.T) {
	server := startServerWithListener(t, &echoListener{}, conf.TLSConfig{})
	defer stopServers(t, server)

	conn, err := createConnection(defaultServerAddress, nil)
	require.NoError(t, err)

	conn.Close()

	handler := newRespHandler()
	err = conn.SendRequestAsync(&clustermsgs.RemotingTestMessage{SomeField: "badgers"}, handler)
	require.Error(t, err)
	require.Equal(t, ErrConnectionClosed, err)
}

func TestUnblockInProgressRequests(t *testing.T) {
	serverListener := &delayingClusterMessageHandler{}
	server := startServerWithListener(t, serverListener, conf.TLSConfig{})
	defer stopServers(t, server)

	serverListener.lock()

	conn, err := createConnection(defaultServerAddress, nil)
	require.NoError(t, err)

	numRequests := 10
	serverListener.wg.Add(numRequests)
	var handlers []*testRespHandler
	for i := 0; i < numRequests; i++ {
		handler := newRespHandler()
		handlers = append(handlers, handler)
		err := conn.SendRequestAsync(&clustermsgs.RemotingTestMessage{SomeField: "badgers"}, handler)
		require.NoError(t, err)
	}

	// We wait for all request to arrive on the server
	serverListener.wg.Wait()

	server.closeNetConns()

	for _, handler := range handlers {
		resp, err := handler.waitForResponse()
		require.Nil(t, resp)
		require.Equal(t, ErrConnectionClosed, err)
	}
	serverListener.unlock()

	conn.Close()
}

func startServerWithListener(t *testing.T, listener ClusterMessageHandler, tlsConf conf.TLSConfig) *server {
	t.Helper()
	return startServerWithListenerAndAddresss(t, listener, defaultServerAddress, tlsConf)
}

func startServerWithListenerAndAddresss(t *testing.T, listener ClusterMessageHandler, address string, tlsConf conf.TLSConfig) *server {
	t.Helper()
	server := newServer(address, tlsConf)
	err := server.Start()
	require.NoError(t, err)
	server.RegisterMessageHandler(ClusterMessageRemotingTestMessage, listener)
	return server
}

func newRespHandler() *testRespHandler {
	handler := &testRespHandler{}
	handler.wg.Add(1)
	return handler
}

type testRespHandler struct {
	resp ClusterMessage
	err  error
	wg   sync.WaitGroup
}

func (t *testRespHandler) HandleResponse(resp ClusterMessage, err error) {
	t.resp = resp
	t.err = err
	t.wg.Done()
}

func (t *testRespHandler) waitForResponse() (ClusterMessage, error) {
	t.wg.Wait()
	return t.resp, t.err
}

func stopServers(t *testing.T, servers ...*server) {
	t.Helper()
	for _, server := range servers {
		err := server.Stop()
		require.NoError(t, err)
	}
}

type echoListener struct {
	calledCount int64
	delay       time.Duration
}

func (e *echoListener) HandleMessage(clusterMessage ClusterMessage) (ClusterMessage, error) {
	if e.delay != 0 {
		time.Sleep(e.delay)
	}
	atomic.AddInt64(&e.calledCount, 1)
	return clusterMessage, nil
}

func (e *echoListener) getCalledCount() int {
	return int(atomic.LoadInt64(&e.calledCount))
}

type returnErrListener struct {
	err   error
	delay time.Duration
}

func (e *returnErrListener) HandleMessage(clusterMessage ClusterMessage) (ClusterMessage, error) {
	if e.delay != 0 {
		time.Sleep(e.delay)
	}
	return nil, e.err
}

type delayingClusterMessageHandler struct {
	m  sync.Mutex
	wg sync.WaitGroup
}

func (d *delayingClusterMessageHandler) lock() {
	d.m.Lock()
}

func (d *delayingClusterMessageHandler) unlock() {
	d.m.Unlock()
}

func (d *delayingClusterMessageHandler) HandleMessage(notification ClusterMessage) (ClusterMessage, error) {
	d.wg.Done()
	d.m.Lock()
	defer d.m.Unlock()
	return &clustermsgs.RemotingTestMessage{SomeField: "foo"}, nil
}
