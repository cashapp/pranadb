package remoting

import (
	"sync"
	"time"
)

func NewFakeServer() *FakeServer {
	return &FakeServer{
		messageHandlers: make(map[ClusterMessageType]ClusterMessageHandler),
	}
}

type FakeServer struct {
	lock            sync.Mutex
	messageHandlers map[ClusterMessageType]ClusterMessageHandler
}

func (f *FakeServer) AvailabilityListener() AvailabilityListener {
	return nil
}

func (f *FakeServer) Start() error {
	return nil
}

func (f *FakeServer) Stop() error {
	return nil
}

func (f *FakeServer) RegisterMessageHandler(notificationType ClusterMessageType, listener ClusterMessageHandler) {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.messageHandlers[notificationType] = listener
}

func (f *FakeServer) BroadcastOneway(notif ClusterMessage) error {
	f.lock.Lock()
	defer f.lock.Unlock()
	listener, ok := f.messageHandlers[TypeForClusterMessage(notif)]
	if !ok {
		panic("no notification listener")
	}
	_, err := listener.HandleMessage(notif)
	return err
}

func (f *FakeServer) BroadcastSync(notif ClusterMessage) error {
	return f.BroadcastOneway(notif)
}

func (f *FakeServer) ConnectionCount() int {
	return 0
}

func (f *FakeServer) SendRequest(notif ClusterMessage, timeout time.Duration) (ClusterMessage, error) {
	return nil, nil
}
