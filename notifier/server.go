package notifier

import (
	"fmt"
	"net"
	"sync"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/common"
)

const (
	readBuffSize                   = 8 * 1024
	messageHeaderSize              = 5 // 1 byte message type, 4 bytes length
	maxConcurrentMsgsPerConnection = 10
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
	listenAddress     string
	listener          net.Listener
	started           bool
	lock              sync.RWMutex
	acceptLoopCh      chan struct{}
	connections       sync.Map
	notifListeners    sync.Map
	responsesDisabled common.AtomicBool
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
		c := s.newConnection(conn)
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

// DisableResponses is used to disable responses - for testing only
func (s *server) DisableResponses() {
	s.responsesDisabled.Set(true)
}

func (s *server) newConnection(conn net.Conn) *connection {
	return &connection{
		s:              s,
		conn:           conn,
		handleMsgCh:    make(chan []byte, maxConcurrentMsgsPerConnection),
		readLoopExitCh: make(chan error, 1),
	}
}

type connection struct {
	s              *server
	conn           net.Conn
	readLoopExitCh chan error
	handleMsgCh    chan []byte
	msgsInProgress sync.WaitGroup
}

func (c *connection) start() {
	go c.readLoop()
	go c.handleMessageLoop()
}

// We execute all messages (other than heartbeat) on a different goroutine, and we use a channel to limit the number
// concurrently executing
func (c *connection) handleMessageLoop() {
	for {
		msg, ok := <-c.handleMsgCh
		if !ok {
			// channel was closed
			return
		}
		go c.handleMessageAsync(msg)
	}
}

func (c *connection) readLoop() {
	readMessage(c.handleMessage, c.readLoopExitCh, c.conn)
	c.s.removeConnection(c)
}

func (c *connection) handleMessage(msgType messageType, msg []byte) {
	if msgType == heartbeatMessageType {
		if !c.s.responsesDisabled.Get() {
			if err := writeMessage(heartbeatMessageType, nil, c.conn); err != nil {
				log.Errorf("failed to write heartbeat %+v", err)
			}
		}
		return
	}
	c.msgsInProgress.Add(1)
	c.handleMsgCh <- msg
}

func (c *connection) handleMessageAsync(msg []byte) {
	c.doHandleMessageAsync(msg)
	c.msgsInProgress.Done()
}

func (c *connection) doHandleMessageAsync(msg []byte) {
	nf := &NotificationMessage{}
	if err := nf.deserialize(msg); err != nil {
		log.Errorf("Failed to deserialize notification %+v", err)
		return
	}
	listener := c.s.lookupNotificationListener(nf.notif)
	err := listener.HandleNotification(nf.notif)
	ok := true
	if err != nil {
		log.Errorf("Failed to handle notification %+v", err)
		ok = false
	}
	if nf.requiresResponse && !c.s.responsesDisabled.Get() {
		if err := c.sendResponse(nf, ok); err != nil {
			log.Errorf("failed to send response %+v", err)
		}
	}
}

func (c *connection) sendResponse(nf *NotificationMessage, ok bool) error {
	resp := &NotificationResponse{
		sequence: nf.sequence,
		ok:       ok,
	}
	buff := resp.serialize(nil)
	err := writeMessage(notificationResponseMessageType, buff, c.conn)
	return err
}

func (c *connection) stop() error {
	if err := c.conn.Close(); err != nil {
		// Do nothing - connection might already have been closed (e.g. from client)
	}
	err, ok := <-c.readLoopExitCh
	if !ok {
		return errors.WithStack(errors.New("connection channel was closed"))
	}
	c.msgsInProgress.Wait() // Wait for all messages to be processed
	close(c.handleMsgCh)
	return err
}
