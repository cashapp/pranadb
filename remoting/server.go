package remoting

import (
	"fmt"
	"net"
	"sync"

	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/errors"
)

const (
	readBuffSize                   = 8 * 1024
	messageHeaderSize              = 5 // 1 byte message type, 4 bytes length
	maxConcurrentMsgsPerConnection = 10
)

type Server interface {
	Start() error

	Stop() error

	RegisterMessageHandler(messageType ClusterMessageType, listener ClusterMessageHandler, directHandle bool)
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

type messageHandlerHolder struct {
	handler ClusterMessageHandler
	direct  bool
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

func (s *server) RegisterMessageHandler(messageType ClusterMessageType, handler ClusterMessageHandler, directHandle bool) {
	_, ok := s.messageHandlers.Load(messageType)
	if ok {
		panic(fmt.Sprintf("message handler with type %d already registered", messageType))
	}
	holder := &messageHandlerHolder{
		handler: handler,
		direct:  directHandle,
	}
	s.messageHandlers.Store(messageType, holder)
}
func (s *server) lookupMessageHandler(clusterMessage ClusterMessage) *messageHandlerHolder {
	l, ok := s.messageHandlers.Load(TypeForClusterMessage(clusterMessage))
	if !ok {
		panic(fmt.Sprintf("no message handler for type %d", TypeForClusterMessage(clusterMessage)))
	}
	return l.(*messageHandlerHolder)
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
		asyncMsgCh:     make(chan *asyncMsgExec, maxConcurrentMsgsPerConnection),
		readLoopExitCh: make(chan error, 1),
	}
}

type connection struct {
	s                   *server
	conn                net.Conn
	readLoopExitCh      chan error
	asyncMsgCh          chan *asyncMsgExec
	asyncMsgsInProgress sync.WaitGroup
	asyncMsgLoopStarted bool
}

func (c *connection) start() {
	go c.readLoop()
}

func (c *connection) readLoop() {
	readMessage(c.handleMessage, c.readLoopExitCh, c.conn)
	c.s.removeConnection(c)
}

// We execute some messages (other than heartbeat) on a different goroutine, and we use a channel to limit the number
// concurrently executing
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

	clusterRequest := &ClusterRequest{}
	if err := clusterRequest.deserialize(msg); err != nil {
		return err
	}
	holder := c.s.lookupMessageHandler(clusterRequest.requestMessage)
	if holder.direct {
		// We process the message directly
		// FIXME - error handling?
		c.processRequest(clusterRequest, holder.handler)
	} else {

		// Start the async message loop if necessary
		if !c.asyncMsgLoopStarted {
			c.asyncMsgLoopStarted = true
			go c.handleMessageLoop()
		}

		// Handle async
		c.asyncMsgsInProgress.Add(1)

		// TODO
		c.asyncMsgCh <- &asyncMsgExec{
			request: clusterRequest,
			handler: holder.handler,
		}
	}
	return nil
}

type asyncMsgExec struct {
	request *ClusterRequest
	handler ClusterMessageHandler
}

func (c *connection) handleMessageAsync(exec *asyncMsgExec) {
	c.processRequest(exec.request, exec.handler)
	c.asyncMsgsInProgress.Done()
}

func (c *connection) processRequest(request *ClusterRequest, handler ClusterMessageHandler) {
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
		resp.errMsg = respErr.Error()
	}
	buff, err := resp.serialize(nil)
	if err != nil {
		return err
	}
	err = writeMessage(responseMessageType, buff, c.conn)
	return errors.WithStack(err)
}

func (c *connection) stop() error {
	if err := c.conn.Close(); err != nil {
		// Do nothing - connection might already have been closed (e.g. from client)
	}
	err, ok := <-c.readLoopExitCh
	if !ok {
		return errors.WithStack(errors.Error("connection channel was closed"))
	}
	c.asyncMsgsInProgress.Wait() // Wait for all async messages to be processed
	close(c.asyncMsgCh)
	return errors.WithStack(err)
}
