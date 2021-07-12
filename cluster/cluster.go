package cluster

import "github.com/squareup/pranadb/common"

type Cluster interface {

	// WriteBatch writes a batch reliability to storage
	WriteBatch(batch *WriteBatch) error

	LocalGet(key []byte) ([]byte, error)

	// LocalScan scans the local store
	LocalScan(startKeyPrefix []byte, whileKeyPrefix []byte, limit int) ([]KVPair, error)

	GetNodeID() int

	GetAllNodeIDs() []int

	GetAllShardIDs() []uint64

	GetLocalShardIDs() []uint64

	// GenerateTableID generates a table using a cluster wide persistent counter
	GenerateTableID() (uint64, error)

	ExecuteRemotePullQuery(schemaName string, query string, queryID string, limit int, nodeID int) chan RemoteQueryResult

	SetRemoteQueryExecutionCallback(callback RemoteQueryExecutionCallback)

	RegisterShardListenerFactory(factory ShardListenerFactory)

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
	ExecuteRemotePullQuery(schemaName string, query string, queryID string, limit int) (*common.Rows, error)
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
