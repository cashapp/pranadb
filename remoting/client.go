package remoting

import (
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/errors"
	"sync"
)

type Client struct {
	connections sync.Map
	lock        sync.Mutex
}

var ErrInsufficientServers = errors.New("insufficient servers available")

func (c *Client) Broadcast(request ClusterMessage, minServers int, serverAddresses ...string) error {
	rh := newBroadcastRespHandler()
	sendCount := 0 //nolint:ifshort
	for _, serverAddress := range serverAddresses {
		conn, err := c.getConnection(serverAddress)
		if err != nil {
			// best effort - ignore the error
			continue
		}
		if err := c.sendRequestWithRetry(conn, request, rh); err != nil {
			continue
		}
		sendCount++
	}
	rh.setRequiredResponses(sendCount)
	if sendCount < minServers {
		return ErrInsufficientServers
	}
	return rh.waitForResponse()
}

func (c *Client) BroadcastOneWay(request ClusterMessage, serverAddresses ...string) {
	for _, serverAddress := range serverAddresses {
		conn, err := c.getConnection(serverAddress)
		if err != nil {
			// best effort - ignore the error
			continue
		}
		if err := c.sendRequestWithRetry(conn, request, nil); err != nil {
			continue
		}
	}
}

func (c *Client) SendRPC(request ClusterMessage, serverAddress string) (ClusterMessage, error) {
	conn, err := c.getConnection(serverAddress)
	if err != nil {
		return nil, err
	}
	rh := &rpcRespHandler{ch: make(chan respHolder, 1)}
	if err := c.sendRequestWithRetry(conn, request, rh); err != nil {
		// Note we do not delete connections on failure - closed connections remain in the map and will be attempted
		// to be recreated next time a request attempt is made. Actively deleting connections introduces a race condition
		// where we could have more than one connection to the same server at same time
		return nil, err
	}

	return rh.waitForResponse()
}

func (c *Client) Stop() {
	c.connections.Range(func(sa, v interface{}) bool {
		cc, ok := v.(*clientConnection)
		if !ok {
			panic("not a clientConnection")
		}
		cc.Close()
		c.connections.Delete(sa)
		return true
	})
}

type respHolder struct {
	resp ClusterMessage
	err  error
}

type rpcRespHandler struct {
	ch chan respHolder
}

func (t *rpcRespHandler) HandleResponse(resp ClusterMessage, err error) {
	t.ch <- respHolder{resp: resp, err: err}
}

func (t *rpcRespHandler) waitForResponse() (ClusterMessage, error) {
	rh := <-t.ch
	return rh.resp, rh.err
}

type broadcastRespHandler struct {
	ch                chan error
	requiredResponses int // The total number of responses required
	respCount         int // The number of responses so far
	lock              common.SpinLock
	err               error
}

func newBroadcastRespHandler() *broadcastRespHandler {
	return &broadcastRespHandler{requiredResponses: -1, ch: make(chan error, 1)}
}

func (t *broadcastRespHandler) setRequiredResponses(requiredResponses int) {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.requiredResponses = requiredResponses
	t.checkSendToChannel()
}

func (t *broadcastRespHandler) HandleResponse(resp ClusterMessage, err error) {
	t.lock.Lock()
	defer t.lock.Unlock()
	if t.err != nil {
		// Already failed
		return
	}
	if err != nil {
		t.err = err
	} else {
		t.respCount++
	}
	t.checkSendToChannel()
}

func (t *broadcastRespHandler) checkSendToChannel() {
	if t.requiredResponses == -1 {
		// Required responses not yet set
		return
	}
	if t.err != nil {
		t.ch <- t.err
	} else if t.respCount == t.requiredResponses {
		t.ch <- nil
	} else if t.respCount > t.requiredResponses {
		panic("too many responses")
	}
}

func (t *broadcastRespHandler) waitForResponse() error {
	err := <-t.ch
	return err
}

func (c *Client) getConnection(serverAddress string) (*clientConnection, error) {
	cc, ok := c.connections.Load(serverAddress)
	var conn *clientConnection
	if !ok {
		var err error
		conn, err = c.maybeCreateAndCacheConnection(serverAddress, nil)
		if err != nil {
			return nil, err
		}
	} else {
		conn, ok = cc.(*clientConnection)
		if !ok {
			panic("not a clientConnection")
		}
	}
	return conn, nil
}

func (c *Client) maybeCreateAndCacheConnection(serverAddress string, oldConn *clientConnection) (*clientConnection, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	cl, ok := c.connections.Load(serverAddress) // check again under the lock - another gr might have created one
	if ok {
		cc, ok := cl.(*clientConnection)
		if !ok {
			panic("not a clientConnection")
		}
		// If we're recreating a connection after a failure, then the old conn will still be in the map - we don't
		// want to return that
		if oldConn == nil || oldConn != cc {
			return cc, nil
		}
	}
	cc, err := createConnection(serverAddress)
	if err != nil {
		return nil, err
	}
	c.connections.Store(serverAddress, cc)
	return cc, nil
}

func (c *Client) sendRequestWithRetry(conn *clientConnection, request ClusterMessage, rh responseHandler) error {
	if err := conn.SendRequestAsync(request, rh); err != nil {
		// It's possible the connection is cached but is closed - e.g. it hasn't been used for some time and has
		// been closed by a NAT / firewall - in this case we will try and connect again
		conn, err = c.maybeCreateAndCacheConnection(conn.serverAddress, conn)
		if err != nil {
			return err
		}
		if err = conn.SendRequestAsync(request, rh); err != nil {
			return err
		}
	}
	return nil
}
