package notifier

import (
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
	BroadcastOneway(notif Notification) error
	BroadcastSync(notif Notification) error
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

// BroadcastSync broadcasts a notification to all nodes and waits until all nodes have responded before returning
func (c *client) BroadcastSync(notif Notification) error {
	nf := c.createNotifcationMessage(notif, true)
	respChan := make(chan bool, len(c.serverAddresses))
	ri := &responseInfo{respChan: respChan, conns: make(map[*clientConnection]struct{})}
	c.responseChannels.Store(nf.sequence, ri)
	if err := c.broadcast(nf, ri); err != nil {
		return errors.WithStack(err)
	}
	ok, k := <-respChan
	if !k {
		return errors.Error("channel was closed")
	}
	c.responseChannels.Delete(nf.sequence)
	if !ok {
		// An error was signalled from the other end
		return errors.Error("failure in processing notification")
	}
	return nil
}

// BroadcastOneway broadcasts a notification to all members of the cluster, and does not wait for responses
// Please note that this is best effort: servers will receive notifications only if they are available.
// Notifications are not persisted and their is no total ordering. Ordering is guaranteed per client instance
// The notifications system is not designed for high volumes of traffic.
func (c *client) BroadcastOneway(notif Notification) error {
	nf := c.createNotifcationMessage(notif, false)
	return c.broadcast(nf, nil)
}

func (c *client) broadcast(nf *NotificationMessage, ri *responseInfo) error {
	c.lock.Lock()
	defer c.lock.Unlock()
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
	// We can't rely on the length of the conns map in the ResponseInfo to determine how many connections have been used for the request
	// As responses can come back before we've finished adding all the connections
	// So we add on count for each server _before_ we send the request and we subtract any request that weren't used
	if ri != nil {
		ri.addToConnCount(int32(len(c.availableServers)))
	}
	for serverAddress := range c.availableServers {
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
				continue
			}
			clientConn = &clientConnection{
				client:        c,
				serverAddress: serverAddress,
				conn:          nc,
			}
			clientConn.start()
			c.connections[serverAddress] = clientConn
		}
		bytes, err := nf.serialize(nil)
		if err != nil {
			maybeRemoveFromConnCount(ri)
			return errors.WithStack(err)
		}
		if err := writeMessage(notificationMessageType, bytes, clientConn.conn); err != nil {
			clientConn.Stop()
			c.makeUnavailable(serverAddress)
			c.connectionClosed(clientConn)
			maybeRemoveFromConnCount(ri)
		} else if ri != nil {
			ri.addConn(clientConn)
		}
	}
	return nil
}

func maybeRemoveFromConnCount(ri *responseInfo) {
	if ri != nil {
		ri.addToConnCount(-1)
	}
}

func (c *client) createNotifcationMessage(notif Notification, requiresResponse bool) *NotificationMessage {
	seq := atomic.AddInt64(&c.msgSeq, 1)
	return &NotificationMessage{
		requiresResponse: requiresResponse,
		sequence:         seq,
		notif:            notif,
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

func serializeNotification(notification Notification) ([]byte, error) {
	b := proto.NewBuffer(nil)
	nt := TypeForNotification(notification)
	if nt == NotificationTypeUnknown {
		return nil, errors.Errorf("invalid notification type %d", nt)
	}
	err := b.EncodeVarint(uint64(nt))
	if err != nil {
		return nil, errors.WithStack(err)
	}
	err = b.Marshal(notification)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return b.Bytes(), nil
}

func (c *client) responseReceived(conn *clientConnection, resp *NotificationResponse) {
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
	lock      sync.Mutex
	respChan  chan bool
	conns     map[*clientConnection]struct{}
	connCount int32
}

func (r *responseInfo) responseReceived(conn *clientConnection, resp *NotificationResponse) {
	r.lock.Lock()
	defer r.lock.Unlock()
	delete(r.conns, conn)
	if !resp.ok {
		// The server received the notification but sent back an error response
		// We return an error from the client call
		r.respChan <- false
		return
	}
	r.addToConnCount(-1)
}

func (r *responseInfo) addToConnCount(val int32) {
	if atomic.AddInt32(&r.connCount, val) == 0 {
		r.respChan <- true
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
	if atomic.LoadInt32(&r.connCount) == 0 {
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
	cc.lock.Lock()
	defer cc.lock.Unlock()
	cc.loopCh = make(chan error, 10)
	go readMessage(cc.handleMessage, cc.loopCh, cc.conn)
	cc.sendHeartbeat()
	cc.started = true
}

func (cc *clientConnection) Stop() {
	cc.stop(true)
}

func (cc *clientConnection) stop(lock bool) {
	if lock {
		cc.lock.Lock()
	}
	cc.started = false
	if cc.hbTimer != nil {
		cc.hbTimer.Stop()
	}
	if lock {
		cc.lock.Unlock()
	}
	if err := cc.conn.Close(); err != nil {
		// Do nothing - connection might already have been closed (e.g. from client)
		log.Errorf("Failed to close connection %+v", err)
	}
	<-cc.loopCh
	cc.client.connectionClosed(cc)
}

func (cc *clientConnection) sendHeartbeat() {
	if err := writeMessage(heartbeatMessageType, nil, cc.conn); err != nil {
		log.Errorf("failed to send heartbeat %+v", err)
		cc.heartbeatFailed()
		return
	}
	cc.hbReceived = false
	t := time.AfterFunc(cc.client.heartbeatInterval, cc.heartTimerFired)
	cc.hbTimer = t
}

func (cc *clientConnection) heartTimerFired() {
	cc.lock.Lock()
	if cc.hbReceived {
		cc.sendHeartbeat()
	} else if cc.started {
		cc.heartbeatFailed()
	}
	cc.lock.Unlock()
}

func (cc *clientConnection) heartbeatFailed() {
	log.Warnf("response heartbeat not received within %f seconds", cc.client.heartbeatInterval.Seconds())
	cc.stop(false)
	cc.client.makeUnavailableWithLock(cc.serverAddress)
	cc.client.connectionClosed(cc)
}

func (cc *clientConnection) heartbeatReceived() {
	cc.lock.Lock()
	cc.hbReceived = true
	cc.lock.Unlock()
}

func (cc *clientConnection) handleMessage(msgType messageType, msg []byte) {
	if msgType == heartbeatMessageType {
		cc.heartbeatReceived()
		return
	}
	if msgType == notificationResponseMessageType {
		resp := &NotificationResponse{}
		resp.deserialize(msg)
		cc.client.responseReceived(cc, resp)
		return
	}
	panic("unexpected message type")
}
