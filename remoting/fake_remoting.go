package remoting

import (
	"sync"
)

func NewFakeServer() *FakeServer {
	return &FakeServer{}
}

type FakeServer struct {
	messageHandlers sync.Map
}

func (f *FakeServer) ServerAddresses() []string {
	return nil
}

func (f *FakeServer) Start() error {
	return nil
}

func (f *FakeServer) Stop() error {
	return nil
}

func (f *FakeServer) RegisterMessageHandler(clusterMsgType ClusterMessageType, listener ClusterMessageHandler) {
	f.messageHandlers.Store(clusterMsgType, listener)
}

func (f *FakeServer) ConnectionCount() int {
	return 0
}

type FakeClient struct {
	server *FakeServer
}

func NewFakeClient(fakeServer *FakeServer) *FakeClient {
	return &FakeClient{server: fakeServer}
}

func (f *FakeClient) Broadcast(clusterMsg ClusterMessage) error {
	l, ok := f.server.messageHandlers.Load(TypeForClusterMessage(clusterMsg))
	if !ok {
		panic("no notification listener")
	}
	listener, ok := l.(ClusterMessageHandler)
	if !ok {
		panic("not a ClusterMessageHandler")
	}
	_, err := listener.HandleMessage(clusterMsg)
	return err
}

func (f *FakeClient) Stop() {
}
