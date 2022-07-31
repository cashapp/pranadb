package remoting

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/errors"
	"net"
	"sync"
	"sync/atomic"
)

type clientConnection struct {
	lock          sync.RWMutex
	netConn       net.Conn
	closeGroup    sync.WaitGroup
	respHandlers  sync.Map
	reqSequence   int64
	closed        bool
	serverAddress string
}

type responseHandler interface {
	HandleResponse(resp ClusterMessage, err error)
}

var ErrConnectionClosed = errors.New("connection closed")

func createConnection(serverAddress string) (*clientConnection, error) {
	netConn, err := createNetConnection(serverAddress)
	if err != nil {
		return nil, err
	}
	cc := &clientConnection{
		netConn:       netConn,
		serverAddress: serverAddress,
	}
	cc.start()
	return cc, nil
}

func (c *clientConnection) SendRequestAsync(message ClusterMessage, respHandler responseHandler) error {
	c.lock.RLock()
	defer c.lock.RUnlock()
	if c.closed {
		return ErrConnectionClosed
	}
	seq := atomic.AddInt64(&c.reqSequence, 1)
	c.respHandlers.Store(seq, respHandler)
	cr := &ClusterRequest{
		requiresResponse: true,
		sequence:         seq,
		requestMessage:   message,
	}
	buf, err := cr.serialize(nil)
	if err != nil {
		return err
	}
	return writeMessage(requestMessageType, buf, c.netConn)
}

func (c *clientConnection) start() {
	c.closeGroup.Add(1)
	go readMessage(c.handleMessage, c.netConn, func() {
		// This will be called after the read loop has exited
		// Note We need to close the connection from this side too, to avoid leak of connections in CLOSE_WAIT state
		c.lock.Lock()
		c.closed = true
		c.lock.Unlock()
		if err := c.netConn.Close(); err != nil {
			// Ignore
		}
		// We notify any waiting response handlers that the connection is closed
		c.respHandlers.Range(func(seq, v interface{}) bool {
			handler, ok := v.(responseHandler)
			if !ok {
				panic("not a responseHandler")
			}
			c.respHandlers.Delete(seq)
			handler.HandleResponse(nil, ErrConnectionClosed)
			return true
		})
		c.closeGroup.Done()
	})
}

func (c *clientConnection) Stop() {
	c.lock.Lock()
	c.closed = true
	c.lock.Unlock() // Note, we must unlock before closing the connection to avoid deadlock
	if err := c.netConn.Close(); err != nil {
		// Do nothing - connection might already have been closed from other side - this is ok
	}
	c.closeGroup.Wait()
}

func (c *clientConnection) ServerAddress() string {
	return c.serverAddress
}

func (c *clientConnection) handleMessage(msgType messageType, msg []byte) error {
	if msgType != responseMessageType {
		panic(fmt.Sprintf("unexpected message type %d msg %v", msgType, msg))
	}
	resp := &ClusterResponse{}
	if err := resp.deserialize(msg); err != nil {
		log.Errorf("failed to deserialize %v", err)
		return err
	}
	r, ok := c.respHandlers.LoadAndDelete(resp.sequence)
	if !ok {
		return errors.New("failed to find response handler")
	}
	handler, ok := r.(responseHandler)
	if !ok {
		panic("not a responseHandler")
	}
	if resp.errMsg != "" {
		respErr := errors.NewPranaError(errors.ErrorCode(resp.errCode), resp.errMsg)
		handler.HandleResponse(nil, respErr)
	} else {
		handler.HandleResponse(resp.responseMessage, nil)
	}
	return nil
}

func createNetConnection(serverAddress string) (net.Conn, error) {
	addr, err := net.ResolveTCPAddr("tcp", serverAddress)
	if err != nil {
		return nil, err
	}
	nc, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		return nil, err
	}
	err = nc.SetNoDelay(true)
	if err != nil {
		return nil, err
	}
	return nc, nil
}
