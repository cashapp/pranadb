package remoting

import (
	"github.com/squareup/pranadb/conf"
	"github.com/squareup/pranadb/errors"
)

// Adapt the current Prana expectations to the new remoting

type Broadcaster interface {
	Broadcast(request ClusterMessage) error
	Stop()
}

var _ Broadcaster = &BroadcastWrapper{}

// BroadcastWrapper is a helper for when you always want to broadcast to the same servers
type BroadcastWrapper struct {
	serverAddresses []string
	client          *Client
}

func NewBroadcastWrapper(tlsConfig conf.TLSConfig, serverAddresses ...string) (*BroadcastWrapper, error) {
	requestClient, err := NewClient(tlsConfig)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &BroadcastWrapper{
		serverAddresses: serverAddresses,
		client:          requestClient,
	}, nil
}

func (b *BroadcastWrapper) Broadcast(request ClusterMessage) error {
	return b.client.Broadcast(request, 1, b.serverAddresses...)
}

func (b *BroadcastWrapper) Stop() {
	b.client.Stop()
}
