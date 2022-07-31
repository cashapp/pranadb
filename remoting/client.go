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
	var conns []*clientConnection
	for _, serverAddress := range serverAddresses {
		conn, err := c.getConnection(serverAddress)
		if err != nil {
			// best effort - ignore the error
			continue
		}
		conns = append(conns, conn)
	}
	rh := &broadcastRespHandler{ch: make(chan error, 1), numResponses: len(conns)}
	sendCount := 0 //nolint:ifshort
	for _, conn := range conns {
		if err := conn.SendRequestAsync(request, rh); err != nil {
			c.connections.Delete(conn.serverAddress)
			// best effort - ignore the error
		} else {
			sendCount++
		}
	}
	if sendCount < minServers {
		return ErrInsufficientServers
	}
	return rh.waitForResponse()
}

func (c *Client) SendRPC(request ClusterMessage, serverAddress string) (ClusterMessage, error) {
	conn, err := c.getConnection(serverAddress)
	if err != nil {
		return nil, err
	}
	rh := &rpcRespHandler{ch: make(chan respHolder, 1)}
	if err := conn.SendRequestAsync(request, rh); err != nil {
		c.connections.Delete(serverAddress)
		return nil, err
	}
	resp, err := rh.waitForResponse()
	if err != nil {
		c.connections.Delete(serverAddress)
	}
	return resp, err
}

func (c *Client) Stop() {
	c.connections.Range(func(sa, v interface{}) bool {
		cc, ok := v.(*clientConnection)
		if !ok {
			panic("not a clientConnection")
		}
		cc.Stop()
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
	ch           chan error
	failed       bool
	numResponses int
	respCount    int
	lock         common.SpinLock
}

func (t *broadcastRespHandler) HandleResponse(resp ClusterMessage, err error) {
	t.lock.Lock()
	defer t.lock.Unlock()
	if t.failed {
		return
	}
	if err != nil {
		t.ch <- err
		t.failed = true
		return
	}
	t.respCount++
	if t.respCount == t.numResponses {
		t.ch <- nil
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
		conn, err = c.maybeCreateAndCacheConnection(serverAddress)
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

func (c *Client) maybeCreateAndCacheConnection(serverAddress string) (*clientConnection, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	cl, ok := c.connections.Load(serverAddress) // check again under the lock
	if ok {
		cc, ok := cl.(*clientConnection)
		if !ok {
			panic("not a clientConnection")
		}
		return cc, nil
	}
	cc, err := createConnection(serverAddress)
	if err != nil {
		return nil, err
	}
	c.connections.Store(serverAddress, cc)
	return cc, nil
}
