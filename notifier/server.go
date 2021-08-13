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
		listenAddress: listenAddress,
		acceptLoopCh:  make(chan struct{}, 1),
	}
}

type server struct {
	listenAddress  string
	listener       net.Listener
	started        bool
	lock           sync.RWMutex
	acceptLoopCh   chan struct{}
	connections    sync.Map
	notifListeners sync.Map
}

func (s *server) Start() error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.started {
		return nil
	}
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
		s.connections.Store(c, struct{}{})
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
	var e error
	s.connections.Range(func(conn, _ interface{}) bool {
		if err := conn.(*connection).stop(); err != nil {
			e = err
			return false
		}
		return true
	})
	s.started = false
	return e
}

func (s *server) ListenAddress() string {
	return s.listenAddress
}

func (s *server) RegisterNotificationListener(notificationType NotificationType, listener NotificationListener) {
	_, ok := s.notifListeners.Load(notificationType)
	if ok {
		panic(fmt.Sprintf("notification listener with type %d already registered", notificationType))
	}
	s.notifListeners.Store(notificationType, listener)
}
func (s *server) lookupNotificationListener(notification Notification) NotificationListener {
	l, ok := s.notifListeners.Load(TypeForNotification(notification))
	if !ok {
		panic(fmt.Sprintf("no notification listener for type %d", TypeForNotification(notification)))
	}
	return l.(NotificationListener)
}

func (s *server) removeConnection(conn *connection) {
	s.connections.Delete(conn)
}

type connection struct {
	s      *server
	conn   net.Conn
	loopCh chan error
}

func (c *connection) start() {
	c.loopCh = make(chan error, 10)
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
				u, _ := common.ReadUint32FromBufferLE(msgBuf, 0)
				msgLen = int(u)
			}
			if len(msgBuf) >= 4+msgLen {
				// We got a whole message
				msg := msgBuf[4 : 4+msgLen]
				nf := &NotificationMessage{}
				if err := nf.deserialize(msg); err != nil {
					c.loopCh <- err
					return
				}
				listener := c.s.lookupNotificationListener(nf.notif)
				listener.HandleNotification(nf.notif)
				if nf.requiresResponse {
					if err := c.sendResponse(nf); err != nil {
						c.loopCh <- err
						return
					}
				}
				// We copy the slice otherwise the backing array won't be gc'd
				msgBuf = common.CopyByteSlice(msgBuf[4+msgLen:])
				msgLen = -1
			} else {
				break
			}
		}
	}
}

func (c *connection) sendResponse(nf *NotificationMessage) error {
	// The response is just the 8 bytes sequence in LE
	buff := make([]byte, 0, 8)
	buff = common.AppendUint64ToBufferLE(buff, uint64(nf.sequence))
	_, err := c.conn.Write(buff)
	return err
}

func (c *connection) stop() error {
	if err := c.conn.Close(); err != nil {
		// Do nothing - connection might already have been closed (e.g. from client)
	}
	err, ok := <-c.loopCh
	if !ok {
		return errors.WithStack(errors.New("connection channel was closed"))
	}
	return err
}
