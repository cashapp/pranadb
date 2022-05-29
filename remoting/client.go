package remoting

import (
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/errors"
)

const (
	connectionRetryBackoff = 1 * time.Second
)

type Client interface {
	SendRequest(message ClusterMessage, timeout time.Duration) (ClusterMessage, error)
	BroadcastOneway(notif ClusterMessage) error
	BroadcastSync(notif ClusterMessage) error
	Start() error
	Stop() error
}

func NewClient(heartbeatInterval time.Duration, serverAddresses ...string) Client {
	return newClient(heartbeatInterval, serverAddresses...)
}

func newClient(heartbeatInterval time.Duration, serverAddresses ...string) *client {
	return &client{
		serverAddresses:    serverAddresses,
		connections:        make(map[string]*clientConnection),
		unavailableServers: make(map[string]time.Time),
		msgSeq:             -1,
		heartbeatInterval:  heartbeatInterval,
	}
}

type client struct {
	started            bool
	serverAddresses    []string
	connections        map[string]*clientConnection
	lock               sync.Mutex
	availableServers   map[string]struct{}
	unavailableServers map[string]time.Time
	responseChannels   sync.Map
	msgSeq             int64
	heartbeatInterval  time.Duration
}

func (c *client) connectionClosed(conn *clientConnection) {
	c.responseChannels.Range(func(_, v interface{}) bool {
		ri, ok := v.(*responseInfo)
		if !ok {
			panic("not a *responseInfo")
		}
		ri.connClosed(conn)
		return true
	})
}

func (c *client) makeUnavailable(serverAddress string) {
	// Cannot write to server or make connection, it's unavailable - it may be down or there's a network issue
	// We remove the server from the set of live servers and add it to the set of unavailable ones
	// Unavailable ones will be retried after a delay
	log.Errorf("Server became unavailable %s", serverAddress)
	delete(c.connections, serverAddress)
	delete(c.availableServers, serverAddress)
	c.unavailableServers[serverAddress] = time.Now()
}

func (c *client) makeUnavailableWithLock(serverAddress string) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.makeUnavailable(serverAddress)
}

// SendRequest attempts to send the request to one of the serverAddresses starting at the first one
func (c *client) SendRequest(requestMessage ClusterMessage, timeout time.Duration) (ClusterMessage, error) {
	nf := c.createRequest(requestMessage, true)
	messageBytes, err := nf.serialize(nil)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	var respChan chan *ClusterResponse
	start := time.Now()
	for {
		respChan = make(chan *ClusterResponse, 10000)
		ri := &responseInfo{
			rpcRespChan: respChan,
			conns:       make(map[*clientConnection]struct{}),
			rpc:         true,
		}
		c.responseChannels.Store(nf.sequence, ri)
		sent := c.doSendRequest(messageBytes, ri, c.serverAddresses...)
		if sent {
			resp, ok := <-respChan
			if !ok {
				return nil, errors.Error("channel was closed")
			}
			if resp != nil {
				c.responseChannels.Delete(nf.sequence)
				if !resp.ok {
					// An error was signalled from the other end
					return nil, errors.New(resp.errMsg)
				}
				return resp.responseMessage, nil
			}
		}
		if time.Now().Sub(start) >= timeout {
			c.responseChannels.Delete(nf.sequence)
			return nil, errors.New("failed to send cluster request - no servers available")
		}
		time.Sleep(1 * time.Second)
		log.Info("retrying")
	}
}

func (c *client) doSendRequest(messageBytes []byte, ri *responseInfo, serverAddresses ...string) bool {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.maybeMakeUnavailableAvailable()
	for _, serverAddress := range serverAddresses {
		if err := c.maybeConnectAndSendMessage(messageBytes, serverAddress, ri); err == nil {
			return true
		}
	}
	return false
}

// BroadcastSync broadcasts a notification to all nodes and waits until all nodes have responded before returning
func (c *client) BroadcastSync(notificationMessage ClusterMessage) error {
	nf := c.createRequest(notificationMessage, true)
	messageBytes, err := nf.serialize(nil)
	if err != nil {
		return errors.WithStack(err)
	}
	respChan := make(chan error, 10000)
	ri := &responseInfo{broadcastRespChan: respChan, conns: make(map[*clientConnection]struct{})}
	c.responseChannels.Store(nf.sequence, ri)
	if err := c.broadcast(messageBytes, ri); err != nil {
		return errors.WithStack(err)
	}
	err, k := <-respChan
	if !k {
		return errors.Error("channel was closed")
	}
	c.responseChannels.Delete(nf.sequence)
	return err
}

// BroadcastOneway broadcasts a notification to all members of the cluster, and does not wait for responses
// Please note that this is best effort: servers will receive notifications only if they are available.
// Notifications are not persisted and their is no total ordering. Ordering is guaranteed per client instance
// The notifications system is not designed for high volumes of traffic.
func (c *client) BroadcastOneway(notificationMessage ClusterMessage) error {
	nf := c.createRequest(notificationMessage, false)
	messageBytes, err := nf.serialize(nil)
	if err != nil {
		return errors.WithStack(err)
	}
	return c.broadcast(messageBytes, nil)
}

func (c *client) broadcast(messageBytes []byte, ri *responseInfo) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.maybeMakeUnavailableAvailable()
	c.incrementConnCount(ri)
	for serverAddress := range c.availableServers {
		if err := c.maybeConnectAndSendMessage(messageBytes, serverAddress, ri); err != nil {
			// Try with the next one
			continue
		}
	}
	// Best effort, not an error if we can't send
	return nil
}

func (c *client) maybeConnectAndSendMessage(messageBytes []byte, serverAddress string, ri *responseInfo) error {
	clientConn, ok := c.connections[serverAddress]
	if !ok {
		var err error
		addr, err := net.ResolveTCPAddr("tcp", serverAddress)
		var nc *net.TCPConn
		if err == nil {
			nc, err = net.DialTCP("tcp", nil, addr)
			if err == nil {
				err = nc.SetNoDelay(true)
			}
		}
		if err != nil {
			log.Warnf("failed to connect to %s %v", serverAddress, err)
			c.makeUnavailable(serverAddress)
			maybeRemoveFromConnCount(ri)
			return err
		}
		clientConn = &clientConnection{
			client:        c,
			serverAddress: serverAddress,
			conn:          nc,
		}
		clientConn.start()
		c.connections[serverAddress] = clientConn
	}
	if err := writeMessage(requestMessageType, messageBytes, clientConn.conn); err != nil {
		clientConn.Stop()
		c.makeUnavailable(serverAddress)
		c.connectionClosed(clientConn)
		maybeRemoveFromConnCount(ri)
		return err
	} else if ri != nil {
		ri.addConn(clientConn)
	}
	return nil
}

func (c *client) incrementConnCount(ri *responseInfo) {
	// We can't rely on the length of the conns map in the ResponseInfo to determine how many connections have been used for the request
	// As responses can come back before we've finished adding all the connections
	// So we add on count for each server _before_ we send the request and we subtract any request that weren't used
	if ri != nil {
		ri.addToConnCount(int32(len(c.availableServers)))
	}
}

func (c *client) maybeMakeUnavailableAvailable() {
	if len(c.unavailableServers) > 0 {
		now := time.Now()
		for serverAddress, failTime := range c.unavailableServers {
			if now.Sub(failTime) >= connectionRetryBackoff {
				// Put the server back in the available set
				log.Warnf("Backoff time for unavailable server %s has expired - adding back to available set", serverAddress)
				delete(c.unavailableServers, serverAddress)
				c.availableServers[serverAddress] = struct{}{}
			}
		}
	}
}

func maybeRemoveFromConnCount(ri *responseInfo) {
	if ri != nil {
		ri.addToConnCount(-1)
	}
}

func (c *client) createRequest(requestMessage ClusterMessage, requiresResponse bool) *ClusterRequest {
	seq := atomic.AddInt64(&c.msgSeq, 1)
	return &ClusterRequest{
		requiresResponse: requiresResponse,
		sequence:         seq,
		requestMessage:   requestMessage,
	}
}

func (c *client) Start() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.started {
		return nil
	}
	c.addAvailableServers()
	c.started = true
	return nil
}

func (c *client) Stop() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if !c.started {
		return nil
	}
	for _, conn := range c.connections {
		conn.Stop()
	}
	c.connections = make(map[string]*clientConnection)
	c.unavailableServers = make(map[string]time.Time)
	c.started = false
	return nil
}

func (c *client) numAvailableServers() int {
	c.lock.Lock()
	defer c.lock.Unlock()

	return len(c.availableServers)
}

func (c *client) numUnavailableServers() int {
	c.lock.Lock()
	defer c.lock.Unlock()

	return len(c.unavailableServers)
}

func (c *client) addAvailableServers() {
	c.availableServers = make(map[string]struct{}, len(c.serverAddresses))
	for _, serverAddress := range c.serverAddresses {
		c.availableServers[serverAddress] = struct{}{}
	}
}

func serializeClusterMessage(clusterMessage ClusterMessage) ([]byte, error) {
	b := proto.NewBuffer(nil)
	nt := TypeForClusterMessage(clusterMessage)
	if nt == ClusterMessageTypeUnknown {
		return nil, errors.Errorf("invalid cluster message type %d", nt)
	}
	err := b.EncodeVarint(uint64(nt))
	if err != nil {
		return nil, errors.WithStack(err)
	}
	err = b.Marshal(clusterMessage)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return b.Bytes(), nil
}

func (c *client) responseReceived(conn *clientConnection, resp *ClusterResponse) {
	r, ok := c.responseChannels.Load(resp.sequence)
	if !ok {
		return
	}
	ri, ok := r.(*responseInfo)
	if !ok {
		panic("expected *responseInfo")
	}
	ri.responseReceived(conn, resp)
}

type responseInfo struct {
	lock              sync.Mutex
	rpcRespChan       chan *ClusterResponse
	broadcastRespChan chan error
	conns             map[*clientConnection]struct{}
	connCount         int32
	rpc               bool
}

func (r *responseInfo) responseReceived(conn *clientConnection, resp *ClusterResponse) {
	if r.rpc {
		r.rpcRespChan <- resp
	} else {
		if !resp.ok {
			// The server received the cluster message but sent back an error response
			r.broadcastRespChan <- errors.Error(resp.errMsg)
		} else {
			r.addToConnCount(-1)
		}
	}
	r.lock.Lock()
	defer r.lock.Unlock()
	delete(r.conns, conn)
}

func (r *responseInfo) addToConnCount(val int32) {
	if r.rpc {
		return
	}
	// A response might already have been received but conn count > 0 because a failed connection hasn't had it's
	// conn count subtracted yet - when it does we might need to trigger the response
	if atomic.AddInt32(&r.connCount, val) == 0 {
		r.broadcastRespChan <- nil
	}
}

func (r *responseInfo) addConn(conn *clientConnection) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.conns[conn] = struct{}{}
}

func (r *responseInfo) connClosed(conn *clientConnection) {
	r.lock.Lock()
	defer r.lock.Unlock()
	if r.rpc {
		r.rpcRespChan <- nil
	}
	if atomic.LoadInt32(&r.connCount) == 0 {
		return
	}
	_, ok := r.conns[conn]
	if !ok {
		return
	}
	delete(r.conns, conn)
	r.addToConnCount(-1)
}

type clientConnection struct {
	lock          sync.Mutex
	client        *client
	serverAddress string
	conn          net.Conn
	loopCh        chan error
	hbTimer       *time.Timer
	hbReceived    bool
	started       bool
}

func (cc *clientConnection) start() {
	if ok := cc.doStart(); !ok {
		// Must be outside the lock
		cc.heartbeatFailed(true)
	}
}

func (cc *clientConnection) doStart() bool {
	cc.lock.Lock()
	defer cc.lock.Unlock()
	cc.loopCh = make(chan error, 10)
	go readMessage(cc.handleMessage, cc.loopCh, cc.conn)
	ok := cc.sendHeartbeat()
	if ok {
		cc.started = true
	}
	return ok
}

func (cc *clientConnection) Stop() {
	cc.stop()
}

func (cc *clientConnection) stop() {
	cc.lock.Lock()
	cc.started = false
	if cc.hbTimer != nil {
		cc.hbTimer.Stop()
	}
	cc.lock.Unlock()
	// We need to execute this and the wait on channel outside the lock to prevent deadlock
	if err := cc.conn.Close(); err != nil {
		// Do nothing - connection might already have been closed (e.g. from client)
		log.Errorf("Failed to close connection %+v", err)
	}
	<-cc.loopCh
	cc.client.connectionClosed(cc)
}

func (cc *clientConnection) sendHeartbeat() bool {
	if err := writeMessage(heartbeatMessageType, nil, cc.conn); err != nil {
		log.Errorf("failed to send heartbeat %+v", err)
		return false
	}
	cc.hbReceived = false
	t := time.AfterFunc(cc.client.heartbeatInterval, cc.heartTimerFired)
	log.Tracef("scheduled heartbeat to fire after %d ms on %s from %s", cc.client.heartbeatInterval.Milliseconds(), cc.conn.LocalAddr().String(),
		cc.conn.RemoteAddr().String())
	cc.hbTimer = t
	return true
}

func (cc *clientConnection) heartTimerFired() {
	failed := false //nolint:ifshort
	cc.lock.Lock()
	log.Tracef("heart timer fired on %s from %s, hb.received is %t", cc.conn.LocalAddr().String(),
		cc.conn.RemoteAddr().String(), cc.hbReceived)
	if cc.hbReceived {
		failed = !cc.sendHeartbeat()
		log.Tracef("heart timer fired then sending another hb on %s from %s, failed %t", cc.conn.LocalAddr().String(),
			cc.conn.RemoteAddr().String(), failed)
	} else if cc.started {
		failed = true
	}
	cc.lock.Unlock()
	if failed {
		// We must execute this outside the lock to prevent a deadlock situation where a heartbeat arrives at the
		// same time this fires
		cc.heartbeatFailed(false)
	}
}

func (cc *clientConnection) heartbeatFailed(atStart bool) {
	log.Warnf("response heartbeat not received within %f seconds on %s from %s at start %t",
		cc.client.heartbeatInterval.Seconds(), cc.conn.LocalAddr().String(), cc.conn.RemoteAddr().String(), atStart)
	cc.stop()
	cc.client.makeUnavailableWithLock(cc.serverAddress)
	cc.client.connectionClosed(cc)
}

func (cc *clientConnection) heartbeatReceived() {
	log.Tracef("response heartbeat response on client %s from %s",
		cc.conn.LocalAddr().String(), cc.conn.RemoteAddr().String())
	cc.lock.Lock()
	cc.hbReceived = true
	cc.lock.Unlock()
	log.Tracef("response heartbeat response on client %s from %s - updated status",
		cc.conn.LocalAddr().String(), cc.conn.RemoteAddr().String())
}

func (cc *clientConnection) handleMessage(msgType messageType, msg []byte) error {
	if msgType == heartbeatMessageType {
		cc.heartbeatReceived()
		return nil
	}
	if msgType == responseMessageType {
		resp := &ClusterResponse{}
		if err := resp.deserialize(msg); err != nil {
			log.Errorf("failed to deserialize %v", err)
			return err
		}
		cc.client.responseReceived(cc, resp)
		return nil
	}
	panic(fmt.Sprintf("unexpected message type %d msg %v", msgType, msg))
}
