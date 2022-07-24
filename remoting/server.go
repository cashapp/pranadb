package remoting

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/errors"
	"net"
	"sync"
)

const (
	readBuffSize                   = 8 * 1024
	messageHeaderSize              = 5 // 1 byte message type, 4 bytes length
	maxConcurrentMsgsPerConnection = 10
)

type Server interface {
	Start() error

	Stop() error

	RegisterMessageHandler(messageType ClusterMessageType, listener ClusterMessageHandler)
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
	messageHandlers   sync.Map
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
	log.Infof("started remoting server on %s", s.listenAddress)
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
	s.connections.Range(func(conn, _ interface{}) bool {
		conn.(*connection).stop()
		return true
	})
	s.started = false
	log.Infof("stopped remoting server on %s", s.listenAddress)
	return nil
}

func (s *server) ListenAddress() string {
	return s.listenAddress
}

func (s *server) RegisterMessageHandler(messageType ClusterMessageType, handler ClusterMessageHandler) {
	_, ok := s.messageHandlers.Load(messageType)
	if ok {
		panic(fmt.Sprintf("message handler with type %d already registered", messageType))
	}
	s.messageHandlers.Store(messageType, handler)
}
func (s *server) lookupMessageHandler(clusterMessage ClusterMessage) ClusterMessageHandler {
	l, ok := s.messageHandlers.Load(TypeForClusterMessage(clusterMessage))
	if !ok {
		panic(fmt.Sprintf("no message handler for type %d", TypeForClusterMessage(clusterMessage)))
	}
	return l.(ClusterMessageHandler)
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
		s:          s,
		conn:       conn,
		asyncMsgCh: make(chan []byte, maxConcurrentMsgsPerConnection),
	}
}

type connection struct {
	s                   *server
	conn                net.Conn
	asyncMsgCh          chan []byte
	asyncMsgsInProgress sync.WaitGroup
	closeGroup          sync.WaitGroup
}

func (c *connection) start() {
	c.closeGroup.Add(1)
	go c.readLoop()
	go c.handleMessageLoop()
}

func (c *connection) readLoop() {
	readMessage(c.handleMessage, c.conn, func() {
		// We need to close the connection from this side too, to avoid leak of connections in CLOSE_WAIT state
		if err := c.conn.Close(); err != nil {
			// Ignore
		}
		// And close the async msg channel
		close(c.asyncMsgCh)
		c.closeGroup.Done()
	})
	c.s.removeConnection(c)
}

// We execute messages (other than heartbeat) on a different goroutine as we need to be able to answer heartbeats even
// when we are processing other messages.
func (c *connection) handleMessageLoop() {
	for {
		exec, ok := <-c.asyncMsgCh
		if !ok {
			// channel was closed
			return
		}
		go c.handleMessageAsync(exec)
	}
}

func (c *connection) handleMessage(msgType messageType, msg []byte) error {
	if msgType == heartbeatMessageType {
		if !c.s.responsesDisabled.Get() {
			if err := writeMessage(heartbeatMessageType, nil, c.conn); err != nil {
				log.Errorf("failed to write heartbeat %+v", err)
			}
		}
		return nil
	}

	// Handle async
	c.asyncMsgsInProgress.Add(1)
	c.asyncMsgCh <- msg

	return nil
}

func (c *connection) handleMessageAsync(msg []byte) {
	c.handleMessageAsync0(msg)
	c.asyncMsgsInProgress.Done()
}

func (c *connection) handleMessageAsync0(msg []byte) {
	request := &ClusterRequest{}
	if err := request.deserialize(msg); err != nil {
		log.Errorf("failed to deserialize message %+v", err)
		return
	}
	handler := c.s.lookupMessageHandler(request.requestMessage)
	respMsg, respErr := handler.HandleMessage(request.requestMessage)
	if respErr != nil {
		log.Errorf("Failed to handle cluster message %+v", respErr)
	}
	if request.requiresResponse && !c.s.responsesDisabled.Get() {
		if err := c.sendResponse(request, respMsg, respErr); err != nil {
			log.Errorf("failed to send response %+v", err)
		}
	}
}

func (c *connection) sendResponse(nf *ClusterRequest, respMsg ClusterMessage, respErr error) error {
	resp := &ClusterResponse{
		sequence:        nf.sequence,
		responseMessage: respMsg,
	}
	if respErr == nil {
		resp.ok = true
	} else {
		resp.ok = false
		var perr errors.PranaError
		if !errors.As(respErr, &perr) {
			perr = common.LogInternalError(respErr)
		}
		resp.errCode = int(perr.Code)
		resp.errMsg = perr.Error()
	}
	buff, err := resp.serialize(nil)
	if err != nil {
		return err
	}
	err = writeMessage(responseMessageType, buff, c.conn)
	return errors.WithStack(err)
}

func (c *connection) stop() {
	if err := c.conn.Close(); err != nil {
		// Do nothing - connection might already have been closed (e.g. from client)
	}
	c.closeGroup.Wait()
	c.asyncMsgsInProgress.Wait() // Wait for all async messages to be processed
}
