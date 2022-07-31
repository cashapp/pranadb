package remoting

// RPCWrapper is a helper for when you always want to RPC to the same server
type RPCWrapper struct {
	serverAddress string
	client        *Client
}

var _ RPCSender = &RPCWrapper{}

// Adapt the current Prana expectations to the new remoting

type Broadcaster interface {
	Broadcast(request ClusterMessage) error
	Stop()
}

var _ Broadcaster = &BroadcastWrapper{}

type RPCSender interface {
	SendRPC(request ClusterMessage) (ClusterMessage, error)
	Stop()
}

func NewRPCWrapper(serverAddress string) *RPCWrapper {
	return &RPCWrapper{
		serverAddress: serverAddress,
		client:        &Client{},
	}
}

func (r *RPCWrapper) SendRPC(request ClusterMessage) (ClusterMessage, error) {
	return r.client.SendRPC(request, r.serverAddress)
}

func (r *RPCWrapper) Stop() {
	r.client.Stop()
}

// BroadcastWrapper is a helper for when you always want to broadcast to the same servers
type BroadcastWrapper struct {
	serverAddresses []string
	client          *Client
}

func NewBroadcastWrapper(serverAddresses ...string) *BroadcastWrapper {
	return &BroadcastWrapper{
		serverAddresses: serverAddresses,
		client:          &Client{},
	}
}

func (b *BroadcastWrapper) Broadcast(request ClusterMessage) error {
	return b.client.Broadcast(request, 1, b.serverAddresses...)
}

func (b *BroadcastWrapper) Stop() {
	b.client.Stop()
}
