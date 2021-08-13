package notifier

import (
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/squareup/pranadb/common"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

const (
	connectionRetryBackoff = 1 * time.Second
	broadcastSyncTimeout   = time.Second * 10
)

type Client interface {
	BroadcastOneway(notif Notification) error
	BroadcastSync(notif Notification) error
	Start() error
	Stop() error
}

func NewClient(serverAddresses ...string) Client {
	return newClient(serverAddresses...)
}

func newClient(serverAddresses ...string) *client {
	return &client{
		serverAddresses:    serverAddresses,
		connections:        make(map[string]*clientConnection),
		unavailableServers: make(map[string]time.Time),
		msgSeq:             -1,
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
}

func (c *client) makeUnavailable(serverAddress string) {
	// Cannot write to server or make connection, it's unavailable - it may be down or there's a network issue
	// We remove the server from the set of live servers and add it to the set of unavailable ones
	// Unavailable ones will be retried after a delay
	log.Printf("Server became unavailable %s", serverAddress)
	delete(c.connections, serverAddress)
	delete(c.availableServers, serverAddress)
	c.unavailableServers[serverAddress] = time.Now()
}

// BroadcastSync broadcasts a notification to all nodes and waits until all nodes have responded before returning
func (c *client) BroadcastSync(notif Notification) error {
	nf := c.createNotifcationMessage(notif, true)
	respChan := make(chan struct{}, 1)
	ri := &responseInfo{respChan: respChan}
	c.responseChannels.Store(nf.sequence, ri)
	if err := c.broadcast(nf); err != nil {
		return err
	}
	select {
	case <-respChan:
	case <-time.After(broadcastSyncTimeout):
		// best effort - if we timeout that's not considered an error
	}
	c.responseChannels.Delete(nf.sequence)
	return nil
}

// BroadcastOneway broadcasts a notification to all members of the cluster, and does not wait for responses
// Please note that this is best effort: servers will receive notifications only if they are available.
// Notifications are not persisted and their is no total ordering. Ordering is guaranteed per client instance
// The notifications system is not designed for high volumes of traffic.
func (c *client) BroadcastOneway(notif Notification) error {
	nf := c.createNotifcationMessage(notif, false)
	return c.broadcast(nf)
}

func (c *client) broadcast(nf *NotificationMessage) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if len(c.unavailableServers) > 0 {
		now := time.Now()
		for serverAddress, failTime := range c.unavailableServers {
			if now.Sub(failTime) >= connectionRetryBackoff {
				// Put the server back in the available set
				log.Printf("Backoff time for unavailable server %s has expired - adding back to available set", serverAddress)
				delete(c.unavailableServers, serverAddress)
				c.availableServers[serverAddress] = struct{}{}
			}
		}
	}
	for serverAddress := range c.availableServers {
		clientConn, ok := c.connections[serverAddress]
		if !ok {
			var err error
			nc, err := net.Dial("tcp", serverAddress)
			if err != nil {
				c.makeUnavailable(serverAddress)
				continue
			}
			clientConn = &clientConnection{
				client: c,
				conn:   nc,
			}
			clientConn.start()
			c.connections[serverAddress] = clientConn
		}
		bytes, err := nf.serialize(nil)
		req := make([]byte, 0, len(bytes)+8)
		req = common.AppendUint32ToBufferLE(req, uint32(len(bytes)))
		req = append(req, bytes...)
		if err != nil {
			return err
		}
		_, err = clientConn.conn.Write(req)
		if err != nil {
			clientConn.stop()
			c.makeUnavailable(serverAddress)
		}
	}
	return nil
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

func (c *client) responseReceived(seq int64) {
	c.lock.Lock()
	defer c.lock.Unlock()
	r, ok := c.responseChannels.Load(seq)
	if !ok {
		return
	}
	ri, ok := r.(*responseInfo)
	if !ok {
		panic("expected *responseInfo")
	}

	numReceived := atomic.AddInt32(&ri.responsesReceived, 1)
	if int(numReceived) == len(c.serverAddresses) {
		ri.respChan <- struct{}{}
	}
}

type responseInfo struct {
	respChan          chan struct{}
	responsesReceived int32
}

type clientConnection struct {
	client *client
	conn   net.Conn
	loopCh chan error
}

func (cc *clientConnection) start() {
	cc.loopCh = make(chan error, 10)
	go cc.readLoop()
}

func (cc *clientConnection) stop() {
	if err := cc.conn.Close(); err != nil {
		// Do nothing - connection might already have been closed (e.g. from client)
	}
	<-cc.loopCh
}

func (cc *clientConnection) readLoop() {
	var msgBuf []byte
	readBuff := make([]byte, readBuffSize)
	for {
		n, err := cc.conn.Read(readBuff)
		if err != nil {
			// Connection closed
			cc.loopCh <- nil
			return
		}
		msgBuf = append(msgBuf, readBuff[0:n]...)
		for len(msgBuf) >= 8 {
			u, _ := common.ReadUint64FromBufferLE(msgBuf, 0)
			seq := int64(u)
			cc.client.responseReceived(seq)
			msgBuf = common.CopyByteSlice(msgBuf[8:])
		}
	}
}
