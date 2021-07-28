package notifier

import (
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/squareup/pranadb/common"
	"log"
	"net"
	"sync"
	"time"
)

const connectionRetryBackoff = 1 * time.Second

type Client interface {
	BroadcastNotification(notif Notification) error
}

func NewClient(serverAddresses ...string) Client {
	return newClient(serverAddresses...)
}

func newClient(serverAddresses ...string) *client {
	availableServers := make(map[string]struct{}, len(serverAddresses))
	for _, serverAddress := range serverAddresses {
		availableServers[serverAddress] = struct{}{}
	}
	return &client{
		serverAddresses:    serverAddresses,
		connections:        make(map[string]net.Conn),
		availableServers:   availableServers,
		unavailableServers: make(map[string]time.Time),
	}
}

type client struct {
	serverAddresses    []string
	connections        map[string]net.Conn
	lock               sync.Mutex
	availableServers   map[string]struct{}
	unavailableServers map[string]time.Time
}

func (c *client) makeUnavailable(serverAddress string) {
	// Cannot write to server or make connection, it's unavailable - it may be down or there's a network issue
	// We remove the server from the set of live servers and add it to the set of unavailable ones
	// Unavailable ones will be retried after a delay
	log.Printf("Failed to send notification to server %s", serverAddress)
	delete(c.connections, serverAddress)
	delete(c.availableServers, serverAddress)
	c.unavailableServers[serverAddress] = time.Now()
}

// BroadcastNotification broadcasts a notification to all members of the cluster.
// Please note that this is best effort: servers will receive notifications only if they are available.
// Notifications are not persisted and their is no total ordering. Ordering is guaranteed per client instance
// The notifications system is not designed for high volumes of traffic.
func (c *client) BroadcastNotification(notif Notification) error {

	bytes, err := serializeNotification(notif)
	if err != nil {
		return err
	}

	bytesToSend := make([]byte, 0, 4+len(bytes))
	bytesToSend = common.AppendUint32ToBufferLittleEndian(bytesToSend, uint32(len(bytes)))
	bytesToSend = append(bytesToSend, bytes...)

	c.lock.Lock()
	defer c.lock.Unlock()

	for serverAddress := range c.availableServers {
		conn, ok := c.connections[serverAddress]
		if !ok {
			var err error
			conn, err = net.Dial("tcp", serverAddress)
			if err != nil {
				c.makeUnavailable(serverAddress)
				continue
			}
			c.connections[serverAddress] = conn
		}
		_, err := conn.Write(bytesToSend)
		if err != nil {
			c.makeUnavailable(serverAddress)
		}
	}
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
	return nil
}

func (c *client) Stop() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	for _, conn := range c.connections {
		if err := conn.Close(); err != nil {
			return err
		}
	}
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
