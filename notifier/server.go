package notifier

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/squareup/pranadb/common"
	"net"
	"sync"
)

const (
	readBuffSize = 8 * 1024
)

type Server interface {
	Start() error

	Stop() error

	RegisterNotificationListener(notificationType NotificationType, listener NotificationListener)
}

func NewServer(listenAddress string) Server {
	return newServer(listenAddress)
}

func newServer(listenAddress string) *server {
	return &server{
		listenAddress:  listenAddress,
		acceptLoopCh:   make(chan struct{}, 1),
		connections:    make(map[*connection]struct{}),
		notifListeners: make(map[NotificationType]NotificationListener),
	}
}

type server struct {
	listenAddress  string
	listener       net.Listener
	started        bool
	lock           sync.RWMutex
	acceptLoopCh   chan struct{}
	connections    map[*connection]struct{}
	notifListeners map[NotificationType]NotificationListener
}

func (s *server) Start() error {
	s.lock.Lock()
	defer s.lock.Unlock()

	list, err := net.Listen("tcp", s.listenAddress)
	if err != nil {
		return errors.WithStack(err)
	}
	s.listener = list
	s.started = true
	go s.acceptLoop()
	return nil
}

func (s *server) acceptLoop() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			// Ok - was closed
			break
		}
		c := &connection{
			conn: conn,
			s:    s,
		}
		s.connections[c] = struct{}{}
		c.start()
	}
	s.acceptLoopCh <- struct{}{}
}

func (s *server) Stop() error {
	s.lock.Lock()
	defer s.lock.Unlock()

	if !s.started {
		return nil
	}
	if err := s.listener.Close(); err != nil {
		return errors.WithStack(err)
	}
	// Wait for accept loop to exit
	_, ok := <-s.acceptLoopCh
	if !ok {
		panic("channel was closed")
	}
	// Now close connections
	for conn := range s.connections {
		if err := conn.stop(); err != nil {
			return err
		}
	}
	s.started = false
	return nil
}

func (s *server) ListenAddress() string {
	return s.listenAddress
}

func (s *server) RegisterNotificationListener(notificationType NotificationType, listener NotificationListener) {
	s.lock.Lock()
	defer s.lock.Unlock()
	_, ok := s.notifListeners[notificationType]
	if ok {
		panic(fmt.Sprintf("notification listener with type %d already registered", notificationType))
	}
	s.notifListeners[notificationType] = listener
}
func (s *server) lookupNotificationListener(notification Notification) NotificationListener {
	s.lock.RLock()
	defer s.lock.RUnlock()
	listener, ok := s.notifListeners[TypeForNotification(notification)]
	if !ok {
		panic(fmt.Sprintf("no notification listener for type %d", TypeForNotification(notification)))
	}
	return listener
}

func (s *server) removeConnection(conn *connection) {
	s.lock.Lock()
	defer s.lock.Unlock()
	delete(s.connections, conn)
}

type connection struct {
	s      *server
	conn   net.Conn
	loopCh chan error
}

func (c *connection) start() {
	c.loopCh = make(chan error, 1)
	go c.readLoop()
}

func (c *connection) readLoop() {
	c.doReadLoop()
	c.s.removeConnection(c)
}

func (c *connection) doReadLoop() {
	var msgBuf []byte
	readBuff := make([]byte, readBuffSize)
	msgLen := -1
	for {
		n, err := c.conn.Read(readBuff)
		if err != nil {
			// Connection closed
			c.loopCh <- nil
			return
		}
		msgBuf = append(msgBuf, readBuff[0:n]...)

		for len(msgBuf) >= 4 {

			if msgLen == -1 {
				msgLen = int(common.ReadUint32FromBufferLittleEndian(msgBuf, 0))
			}
			if len(msgBuf)+4 >= msgLen {
				// We got a whole message
				msg := msgBuf[4 : 4+msgLen]
				notification, err := DeserializeNotification(msg)
				if err != nil {
					c.loopCh <- err
					return
				}
				listener := c.s.lookupNotificationListener(notification)
				listener.HandleNotification(notification)

				msgBuf = msgBuf[4+msgLen:]
				msgLen = -1
			} else {
				break
			}
		}
	}
}

func (c *connection) stop() error {
	if err := c.conn.Close(); err != nil {
		// Do nothing - connection might already have been closed (e.g. from client)
	}
	// What for connection loop to stop
	err, ok := <-c.loopCh
	if !ok {
		return errors.WithStack(errors.New("connection channel was closed"))
	}
	return err
}
