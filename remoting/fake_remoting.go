package remoting

import (
	log "github.com/sirupsen/logrus"
	"sync"
	"time"
)

func NewFakeServer() *FakeServer {
	return &FakeServer{}
}

type FakeServer struct {
	messageHandlers sync.Map
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
	f.messageHandlers.Store(notificationType, listener)
}

func (f *FakeServer) BroadcastOneway(notif ClusterMessage) error {
	go func() {
		if err := f.BroadcastSync(notif); err != nil {
			log.Errorf("failed to handle notification %+v", err)
		}
	}()
	return nil
}

func (f *FakeServer) BroadcastSync(notif ClusterMessage) error {
	l, ok := f.messageHandlers.Load(TypeForClusterMessage(notif))
	if !ok {
		panic("no notification listener")
	}
	listener, ok := l.(ClusterMessageHandler)
	if !ok {
		panic("not a ClusterMessageHandler")
	}
	_, err := listener.HandleMessage(notif)
	return err
}

func (f *FakeServer) ConnectionCount() int {
	return 0
}

func (f *FakeServer) SendRequest(notif ClusterMessage, timeout time.Duration) (ClusterMessage, error) {
	return nil, nil
}
