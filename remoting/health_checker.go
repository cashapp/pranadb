package remoting

import (
	log "github.com/sirupsen/logrus"
	"math/rand"
	"net"
	"sync"
	"time"
)

func NewHealthChecker(serverAddresses []string, hbTimeout time.Duration, hbInterval time.Duration) *HealthChecker {
	return &HealthChecker{
		serverAddresses: serverAddresses,
		hbTimeout:       hbTimeout,
		hbInterval:      hbInterval,
		connections:     map[string]net.Conn{},
	}
}

type AvailabilityListener interface {
	AvailabilityChanged(serverAddresses []string)
}

type HealthChecker struct {
	started         bool
	serverAddresses []string
	connections     map[string]net.Conn
	availListeners  []AvailabilityListener
	hbTimeout       time.Duration
	hbInterval      time.Duration
	timer           *time.Timer
	lock            sync.Mutex
	firstSchedule   bool
}

func (h *HealthChecker) AddAvailabilityListener(listener AvailabilityListener) {
	h.lock.Lock()
	defer h.lock.Unlock()
	h.availListeners = append(h.availListeners, listener)
	listener.AvailabilityChanged(h.getAvailableServers())
}

func (h *HealthChecker) Start() {
	h.lock.Lock()
	defer h.lock.Unlock()
	if h.started {
		return
	}
	h.started = true
	h.checkConnections()
}

func (h *HealthChecker) Stop() {
	h.lock.Lock()
	defer h.lock.Unlock()
	if !h.started {
		return
	}
	h.started = false
	if h.timer != nil {
		h.timer.Stop()
	}
	for _, conn := range h.connections {
		if err := conn.Close(); err != nil {
			// Ignore
		}
	}
	h.firstSchedule = false
}

func (h *HealthChecker) checkConnections() {
	if !h.started {
		return
	}

	chans := make([]chan net.Conn, len(h.serverAddresses))
	for i, serverAddress := range h.serverAddresses {
		// We do the checks in parallel
		ch := make(chan net.Conn)
		chans[i] = ch
		conn, _ := h.connections[serverAddress]
		go h.checkConnectionWithChan(conn, serverAddress, ch)
	}
	changeOccurred := false
	for i, ch := range chans {
		conn := <-ch
		serverAddress := h.serverAddresses[i]
		_, prev := h.connections[serverAddress]
		if !prev && conn != nil {
			// New connection added
			h.connections[serverAddress] = conn
			changeOccurred = true
		} else if prev && conn == nil {
			// Connection closed
			delete(h.connections, serverAddress)
			changeOccurred = true
		}
	}
	if changeOccurred {
		h.sendAvailabilityUpdate()
	}

	var delay time.Duration
	if !h.firstSchedule {
		// Randomise the first schedule completely
		delay = time.Duration(float64(h.hbInterval) * rand.Float64())
		h.firstSchedule = true
	} else {
		// We add a random element (+/- 25%) to the delay to prevent them all synchronizing over time
		randomDelay := 0.5 * float64(h.hbInterval) * (rand.Float64() - 0.5)
		delay = time.Duration(randomDelay) + h.hbInterval
	}

	h.timer = time.AfterFunc(delay, h.checkConnectionsWithLock)
}

func (h *HealthChecker) sendAvailabilityUpdate() {
	addresses := h.getAvailableServers()
	for _, listener := range h.availListeners {
		listener.AvailabilityChanged(addresses)
	}
}

func (h *HealthChecker) getAvailableServers() []string {
	addresses := make([]string, len(h.connections))
	i := 0
	for address := range h.connections {
		addresses[i] = address
		i++
	}
	return addresses
}

func (h *HealthChecker) checkConnectionsWithLock() {
	h.lock.Lock()
	defer h.lock.Unlock()
	h.checkConnections()
}

func (h *HealthChecker) checkConnectionWithChan(conn net.Conn, serverAddress string, ch chan net.Conn) {
	nc := h.checkConnection(conn, serverAddress)
	ch <- nc
}

func (h *HealthChecker) checkConnection(conn net.Conn, serverAddress string) net.Conn {
	if conn == nil {
		var err error
		conn, err = h.createConnection(serverAddress)
		if err != nil {
			log.Infof("health checker failed to connect to %s", serverAddress)
			return nil
		}
		log.Infof("health checker connected to %s", serverAddress)
	}
	if err := h.heartbeat(conn); err != nil {
		log.Infof("heartbeat returned err %v", err)
		if err := conn.Close(); err != nil {
			// Ignore
		}
		return nil
	}
	return conn
}

func (h *HealthChecker) createConnection(serverAddress string) (net.Conn, error) {
	addr, err := net.ResolveTCPAddr("tcp", serverAddress)
	if err != nil {
		return nil, err
	}
	nc, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		return nil, err
	}
	err = nc.SetNoDelay(true)
	if err != nil {
		return nil, err
	}
	return nc, nil
}

func (h *HealthChecker) heartbeat(conn net.Conn) error {
	_, err := conn.Write([]byte{heartbeatMessageType})
	if err != nil {
		return err
	}
	readBuff := make([]byte, 1)
	err = conn.SetReadDeadline(time.Now().Add(h.hbTimeout))
	if err != nil {
		return err
	}
	_, err = conn.Read(readBuff)
	if err != nil {
		return err
	}
	if readBuff[0] != heartbeatMessageType {
		panic("not a heartbeat")
	}
	return nil
}
