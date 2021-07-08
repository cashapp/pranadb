package cluster

import "github.com/squareup/pranadb/common"

type Cluster interface {

	// WriteBatch writes a batch reliability to storage
	WriteBatch(batch *WriteBatch) error

	LocalGet(key []byte) ([]byte, error)

	// LocalScan scans the local store
	LocalScan(startKeyPrefix []byte, whileKeyPrefix []byte, limit int) ([]KVPair, error)

	SetRemoteWriteHandler(handler RemoteWriteHandler)

	GetNodeID() int

	GetAllNodeIDs() []int

	GetAllShardIDs() []uint64

	// GenerateTableID generates a table using a cluster wide persistent counter
	GenerateTableID() (uint64, error)

	// SetLeaderChangedCallback - LeaderChangeCallback is called when this node becomes leader / stops being a leader for a shard
	SetLeaderChangedCallback(callback LeaderChangeCallback)

	ExecuteRemotePullQuery(schemaName string, query string, queryID string, limit int, nodeID int) chan RemoteQueryResult

	SetRemoteQueryExecutionCallback(callback RemoteQueryExecutionCallback)

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
