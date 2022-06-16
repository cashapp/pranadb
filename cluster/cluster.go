package cluster

import (
	"github.com/squareup/pranadb/common"
)

const (
	// SystemSchemaShardID is a shard for storing system tables. Note that this actually writes metadata to
	// the first data shard.
	SystemSchemaShardID uint64 = 1000

	// DataShardIDBase is the lowest value of a data shard id
	DataShardIDBase uint64 = 1000
)

type Cluster interface {

	// WriteBatch writes a batch reliably to storage
	WriteBatch(batch *WriteBatch) error

	// WriteForwardBatch writes a batch reliably for forwarding to another shard
	WriteForwardBatch(batch *WriteBatch) error

	// WriteBatchLocally writes a batch directly using the KV store without going through Raft
	WriteBatchLocally(batch *WriteBatch) error

	LocalGet(key []byte) ([]byte, error)

	// LocalScan scans the local store
	// endKeyPrefix is exclusive
	LocalScan(startKeyPrefix []byte, endKeyPrefix []byte, limit int) ([]KVPair, error)

	CreateSnapshot() (Snapshot, error)

	LocalScanWithSnapshot(snapshot Snapshot, startKeyPrefix []byte, endKeyPrefix []byte, limit int) ([]KVPair, error)

	GetNodeID() int

	GetAllShardIDs() []uint64

	// GetLocalShardIDs returns the ids of the shards on the local node - this includes replicas
	GetLocalShardIDs() []uint64

	// GenerateClusterSequence generates a cluster wide unique sequence number
	GenerateClusterSequence(sequenceName string) (uint64, error)

	SetRemoteQueryExecutionCallback(callback RemoteQueryExecutionCallback)

	RegisterShardListenerFactory(factory ShardListenerFactory)

	ExecuteRemotePullQuery(queryInfo *QueryExecutionInfo, rowsFactory *common.RowsFactory) (*common.Rows, error)

	DeleteAllDataInRangeForAllShardsLocally(startPrefix []byte, endPrefix []byte) error

	DeleteAllDataInRangeForShardLocally(shardID uint64, startPrefix []byte, endPrefix []byte) error

	GetLock(prefix string) (bool, error)

	ReleaseLock(prefix string) (bool, error)

	AddToDeleteBatch(batch *ToDeleteBatch) error

	RemoveToDeleteBatch(batch *ToDeleteBatch) error

	Start() error

	Stop() error

	PostStartChecks(queryExec common.SimpleQueryExec) error
}

type ToDeleteBatch struct {
	ConditionalTableID uint64
	Prefixes           [][]byte
}

type Snapshot interface {
	Close()
}

type QueryExecutionInfo struct {
	ExecutionID string
	SchemaName  string
	Query       string
	Limit       uint32
	ShardID     uint64
	SystemQuery bool
}

func (q *QueryExecutionInfo) Serialize(buff []byte) ([]byte, error) {
	buff = common.AppendStringToBufferLE(buff, q.ExecutionID)
	buff = common.AppendStringToBufferLE(buff, q.SchemaName)
	buff = common.AppendStringToBufferLE(buff, q.Query)
	buff = common.AppendUint32ToBufferLE(buff, q.Limit)
	buff = common.AppendUint64ToBufferLE(buff, q.ShardID)
	var b byte
	if q.SystemQuery {
		b = 1
	} else {
		b = 0
	}
	buff = append(buff, b)
	return buff, nil
}

func (q *QueryExecutionInfo) Deserialize(buff []byte) error {
	offset := 0
	q.ExecutionID, offset = common.ReadStringFromBufferLE(buff, offset)
	q.SchemaName, offset = common.ReadStringFromBufferLE(buff, offset)
	q.Query, offset = common.ReadStringFromBufferLE(buff, offset)
	q.Limit, offset = common.ReadUint32FromBufferLE(buff, offset)
	q.ShardID, offset = common.ReadUint64FromBufferLE(buff, offset)
	q.SystemQuery = buff[offset] == 1
	return nil
}

type RemoteQueryResult struct {
	Rows *common.Rows
	Err  error
}

type LeaderChangeCallback interface {
	LeaderChanged(shardID uint64, added bool)
}

type RemoteQueryExecutionCallback interface {
	ExecuteRemotePullQuery(queryInfo *QueryExecutionInfo) (*common.Rows, error)
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
