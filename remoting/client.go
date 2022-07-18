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

type Client interface {
	SendRequest(message ClusterMessage, timeout time.Duration) (ClusterMessage, error)
	BroadcastOneway(notif ClusterMessage) error
	BroadcastSync(notif ClusterMessage) error
	Start() error
	Stop() error
	AvailabilityListener() AvailabilityListener
}

func NewClient(alwaysTryConnect bool, serverAddresses ...string) Client {
	return newClient(alwaysTryConnect, serverAddresses...)
}

func newClient(alwaysTryConnect bool, serverAddresses ...string) *client {
	return &client{
		serverAddresses:    serverAddresses,
		connections:        make(map[string]*clientConnection),
		unavailableServers: make(map[string]struct{}),
		msgSeq:             -1,
		alwaysTryConnect:   alwaysTryConnect,
	}
}

type client struct {
	ccIDSeq            int64
	started            bool
	serverAddresses    []string
	connections        map[string]*clientConnection
	lock               sync.Mutex
	availableServers   map[string]struct{}
	unavailableServers map[string]struct{}
	responseChannels   sync.Map
	msgSeq             int64
	alwaysTryConnect   bool
}

func (c *client) AvailabilityListener() AvailabilityListener {
	return c
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

func (c *client) AvailabilityChanged(availServers []string) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.availableServers = make(map[string]struct{}, len(availServers))
	for _, serverAddress := range availServers {
		c.availableServers[serverAddress] = struct{}{}
	}
	c.unavailableServers = map[string]struct{}{}
	for _, address := range c.serverAddresses {
		_, ok := c.availableServers[address]
		if !ok {
			c.unavailableServers[address] = struct{}{}
		}
	}
	for address, conn := range c.connections {
		_, ok := c.availableServers[address]
		if !ok {
			delete(c.connections, address)
			conn.stop()
		}
	}
}

func (c *client) maybeMakeUnavailable(serverAddress string) {
	// Cannot write to server or make connection, it's unavailable - it may be down or there's a network issue
	// We remove the server from the set of live servers and add it to the set of unavailable ones
	// We only do this if alwaysTryConnect is false, otherwise we always try and connect every time
	if !c.alwaysTryConnect {
		log.Debugf("Server became unavailable %s", serverAddress)
		delete(c.connections, serverAddress)
		delete(c.availableServers, serverAddress)
		c.unavailableServers[serverAddress] = struct{}{}
	}
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
		sent := c.doSendRequest(messageBytes, ri)
		if sent {
			resp, ok := <-respChan
			if !ok {
				return nil, errors.Error("channel was closed")
			}
			if resp != nil {
				c.responseChannels.Delete(nf.sequence)
				if !resp.ok {
					// An error was signalled from the other end
					return nil, errors.NewPranaError(errors.ErrorCode(resp.errCode), resp.errMsg)
				}
				return resp.responseMessage, nil
			}
		}
		if time.Now().Sub(start) >= timeout {
			c.responseChannels.Delete(nf.sequence)
			return nil, errors.New("failed to send cluster request - no servers available")
		}
		time.Sleep(1 * time.Second)
	}
}

func (c *client) doSendRequest(messageBytes []byte, ri *responseInfo) bool {
	c.lock.Lock()
	defer c.lock.Unlock()
	if !c.started {
		panic("not started")
	}
	for _, serverAddress := range c.serverAddresses {
		_, ok := c.availableServers[serverAddress]
		if ok {
			if err := c.maybeConnectAndSendMessage(messageBytes, serverAddress, ri); err == nil {
				return true
			} else { //nolint:revive
				log.Warnf("failed to send request %v", err)
			}
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
// Notifications are not persisted and there is no total ordering. Ordering is guaranteed per client instance
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

func (c *client) createConnection(serverAddress string) (net.Conn, error) {
	addr, err := net.ResolveTCPAddr("tcp", serverAddress)
	if err != nil {
		log.Errorf("lookup failed! %s %+v", serverAddress, err)
		return nil, err
	}
	nc, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		log.Errorf("dial failed! %v %+v", addr, err)
		return nil, err
	}
	err = nc.SetNoDelay(true)
	if err != nil {
		return nil, err
	}
	return nc, nil
}

func (c *client) maybeConnectAndSendMessage(messageBytes []byte, serverAddress string, ri *responseInfo) error {
	clientConn, ok := c.connections[serverAddress]
	if !ok {
		nc, err := c.createConnection(serverAddress)
		if err != nil {
			log.Warnf("failed to connect to %s %v", serverAddress, err)
			c.maybeMakeUnavailable(serverAddress)
			maybeRemoveFromConnCount(ri)
			return err
		}
		clientConn = &clientConnection{
			id:            atomic.AddInt64(&c.ccIDSeq, 1),
			client:        c,
			serverAddress: serverAddress,
			conn:          nc,
		}
		clientConn.start()
		c.connections[serverAddress] = clientConn
	}
	if err := writeMessage(requestMessageType, messageBytes, clientConn.conn); err != nil {
		clientConn.Stop()
		c.maybeMakeUnavailable(serverAddress)
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
	c.unavailableServers = make(map[string]struct{})
	c.availableServers = make(map[string]struct{})
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
			err := errors.NewPranaError(errors.ErrorCode(resp.errCode), resp.errMsg)
			r.broadcastRespChan <- err
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
	id            int64
	lock          sync.Mutex
	client        *client
	serverAddress string
	loopCh        chan error
	hbTimer       *time.Timer
	started       bool
	conn          net.Conn
}

func (cc *clientConnection) start() {
	cc.lock.Lock()
	defer cc.lock.Unlock()
	cc.loopCh = make(chan error, 10)
	go readMessage(cc.handleMessage, cc.loopCh, cc.conn, func() {
		// Connection closed
		// We need to close the connection from this side too, to avoid leak of connections in CLOSE_WAIT state
		if err := cc.conn.Close(); err != nil {
			// Ignore
		}
	})
	cc.started = true
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
		// Do nothing - connection might already have been closed from other side - this is ok
	}
	<-cc.loopCh
	cc.client.connectionClosed(cc)
}

func (cc *clientConnection) handleMessage(msgType messageType, msg []byte) error {
	if msgType == heartbeatMessageType {
		panic("received heartbeat on client")
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
