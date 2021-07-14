package cluster

import (
	"github.com/squareup/pranadb/common"
)

const (
	UserTableIDBase = 100
)

type Cluster interface {

	// WriteBatch writes a batch reliability to storage
	WriteBatch(batch *WriteBatch) error

	LocalGet(key []byte) ([]byte, error)

	// LocalScan scans the local store
	LocalScan(startKeyPrefix []byte, whileKeyPrefix []byte, limit int) ([]KVPair, error)

	GetNodeID() int

	GetAllShardIDs() []uint64

	GetLocalShardIDs() []uint64

	// GenerateTableID generates a table using a cluster wide persistent counter
	GenerateTableID() (uint64, error)

	ExecuteRemotePullQuery(schemaName string, query string, queryID string, limit int, shardID uint64, rowsFactory *common.RowsFactory) (*common.Rows, error)

	SetRemoteQueryExecutionCallback(callback RemoteQueryExecutionCallback)

	RegisterShardListenerFactory(factory ShardListenerFactory)

	BroadcastNotification(notification Notification) error

	RegisterNotificationListener(notificationType NotificationType, listener NotificationListener)

	Start() error

	Stop() error
}

type RemoteQueryResult struct {
	Rows *common.Rows
	Err  error
}

type LeaderChangeCallback interface {
	LeaderChanged(shardID uint64, added bool)
}

type RemoteQueryExecutionCallback interface {
	ExecuteRemotePullQuery(schemaName string, query string, queryID string, limit int, shardID uint64) (*common.Rows, error)
}

// RemoteWriteHandler will be called when a remote write is done to a shard
type RemoteWriteHandler interface {
	RemoteWriteOccurred(shardID uint64)
}

type ShardCallback interface {
	Write(batch WriteBatch) error
}

type KVPair struct {
	Key   []byte
	Value []byte
}

type ShardListenerFactory interface {
	CreateShardListener(shardID uint64) ShardListener
}

type ShardListener interface {
	RemoteWriteOccurred()

	Close()
}
