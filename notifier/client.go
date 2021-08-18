package notifier

import (
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/squareup/pranadb/common"
	"go.uber.org/zap"
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

func NewClient(logger *zap.Logger, heartbeatInterval time.Duration, serverAddresses ...string) Client {
	return newClient(logger, heartbeatInterval, serverAddresses...)
}

func newClient(logger *zap.Logger, heartbeatInterval time.Duration, serverAddresses ...string) *client {
	return &client{
		logger:             logger,
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
	logger             *zap.Logger
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
	c.logger.Info("Server became unavailable", zap.String("addr", serverAddress))
	delete(c.connections, serverAddress)
	delete(c.availableServers, serverAddress)
	c.unavailableServers[serverAddress] = time.Now()
}

// BroadcastSync broadcasts a notification to all nodes and waits until all nodes have responded before returning
func (c *client) BroadcastSync(notif Notification) error {
	nf := c.createNotifcationMessage(notif, true)
	respChan := make(chan bool, 1)
	ri := &responseInfo{respChan: respChan, conns: make(map[*clientConnection]struct{})}
	c.responseChannels.Store(nf.sequence, ri)
	if err := c.broadcast(nf, ri); err != nil {
		return err
	}
	ok, k := <-respChan
	if !k {
		return errors.New("channel was closed")
	}
	c.responseChannels.Delete(nf.sequence)
	if !ok {
		// An error was signalled from the other end
		return errors.New("failure in processing notification")
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
				c.logger.Sugar().Warn("Backoff time for unavailable server %s has expired - adding back to available set", serverAddress)
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
			nc, err := net.Dial("tcp", serverAddress)
			if err != nil {
				c.makeUnavailable(serverAddress)
				maybeRemoveFromConnCount(ri)
				continue
			}
			clientConn = &clientConnection{
				client:        c,
				serverAddress: serverAddress,
				conn:          nc,
				logger:        c.logger,
			}
			clientConn.start()
			c.connections[serverAddress] = clientConn
		}
		bytes, err := nf.serialize(nil)
		if err != nil {
			maybeRemoveFromConnCount(ri)
			return err
		}
		if err := writeMessage(notificationMessageType, bytes, clientConn.conn); err != nil {
			clientConn.stop()
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
		conn.stop()
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
	client        *client
	serverAddress string
	conn          net.Conn
	loopCh        chan error
	hbTimer       atomic.Value
	hbReceived    common.AtomicBool
	logger        *zap.Logger
}

func (cc *clientConnection) setHbTimer(t *time.Timer) {
	cc.hbTimer.Store(t)
}

func (cc *clientConnection) getHbTimer() *time.Timer {
	v := cc.hbTimer.Load()
	t, ok := v.(*time.Timer)
	if !ok {
		panic("not a pointer to a timer")
	}
	return t
}

func (cc *clientConnection) start() {
	cc.loopCh = make(chan error, 10)
	go readMessage(cc.handleMessage, cc.loopCh, cc.conn)
	cc.sendHeartbeat()
}

func (cc *clientConnection) stop() {
	if t := cc.getHbTimer(); t != nil {
		t.Stop()
	}
	if err := cc.conn.Close(); err != nil {
		// Do nothing - connection might already have been closed (e.g. from client)
		cc.logger.Warn("Failed to close connection", zap.Error(err))
	}
	<-cc.loopCh
	cc.client.connectionClosed(cc)
}

func (cc *clientConnection) sendHeartbeat() {
	if err := writeMessage(heartbeatMessageType, nil, cc.conn); err != nil {
		cc.logger.Error("failed to send heartbeat", zap.Error(err))
		cc.heartbeatFailed()
		return
	}
	cc.hbReceived.Set(false)
	t := time.AfterFunc(cc.client.heartbeatInterval, func() {
		if cc.hbReceived.Get() {
			cc.sendHeartbeat()
		} else {
			cc.logger.Error(fmt.Sprintf("response heartbeat not received within %f seconds", cc.client.heartbeatInterval.Seconds()))
			cc.heartbeatFailed()
		}
	})
	cc.setHbTimer(t)
}

func (cc *clientConnection) heartbeatFailed() {
	cc.stop()
	cc.client.makeUnavailable(cc.serverAddress)
	cc.client.connectionClosed(cc)
}

func (cc *clientConnection) heartbeatReceived() {
	cc.hbReceived.Set(true)
}

func (cc *clientConnection) handleMessage(msgType messageType, msg []byte) error {
	if msgType == heartbeatMessageType {
		cc.heartbeatReceived()
		return nil
	}
	if msgType == notificationResponseMessageType {
		resp := &NotificationResponse{}
		if err := resp.deserialize(msg); err != nil {
			return err
		}
		cc.client.responseReceived(cc, resp)
		return nil
	}
	panic("unexpected message type")
}
