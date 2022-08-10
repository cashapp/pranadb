package cluster

import (
	"fmt"

	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/errors"
)

const (
	// SystemSchemaShardID is a shard for storing system tables. Note that this actually writes metadata to
	// the first data shard.
	SystemSchemaShardID uint64 = 1000

	// DataShardIDBase is the lowest value of a data shard id
	DataShardIDBase uint64 = 1000
)

type Cluster interface {
	ExecuteForwardBatch(shardID uint64, batch []byte) error

	// WriteBatch writes a batch reliably to storage
	WriteBatch(batch *WriteBatch, localOnly bool) error

	// WriteForwardBatch writes a batch reliably for forwarding to another shard
	WriteForwardBatch(batch *WriteBatch, localOnly bool) error

	// WriteBatchLocally writes a batch directly using the KV store without going through Raft
	WriteBatchLocally(batch *WriteBatch) error

	LocalGet(key []byte) ([]byte, error)

	LinearizableGet(shardID uint64, key []byte) ([]byte, error)

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

	CheckConstantReplicationFactor(expectedReplicationFactor int) error

	CheckConstantShards(expectedReplicationFactor int) error

	Start() error

	Stop() error

	PostStartChecks(queryExec common.SimpleQueryExec) error

	SyncStore() error
}

type ToDeleteBatch struct {
	ConditionalTableID uint64
	Prefixes           [][]byte
}

type Snapshot interface {
	Close()
}

type QueryExecutionInfo struct {
	ExecutionID       string
	SchemaName        string
	Query             string
	Limit             uint32
	ShardID           uint64
	SystemQuery       bool
	PsArgTypes        []common.ColumnType
	PsArgs            []interface{}
	PreparedStatement bool
}

func (q *QueryExecutionInfo) Serialize(buff []byte) ([]byte, error) {
	buff = common.AppendStringToBufferLE(buff, q.ExecutionID)
	buff = common.AppendStringToBufferLE(buff, q.SchemaName)
	buff = common.AppendStringToBufferLE(buff, q.Query)
	buff = common.AppendUint32ToBufferLE(buff, q.Limit)
	buff = common.AppendUint64ToBufferLE(buff, q.ShardID)
	buff = appendBool(buff, q.SystemQuery)
	buff = appendBool(buff, q.PreparedStatement)
	if q.PreparedStatement {
		buff = common.AppendUint32ToBufferLE(buff, uint32(len(q.PsArgTypes)))
		for _, argType := range q.PsArgTypes {
			buff = append(buff, byte(argType.Type))
			if argType.Type == common.TypeDecimal {
				buff = append(buff, byte(argType.DecPrecision))
				buff = append(buff, byte(argType.DecScale))
			} else if argType.Type == common.TypeTimestamp {
				buff = append(buff, byte(argType.FSP))
			}
		}
		for i, argType := range q.PsArgTypes {
			arg := q.PsArgs[i]
			switch argType.Type {
			case common.TypeTinyInt, common.TypeInt, common.TypeBigInt:
				i64val, ok := arg.(int64)
				if !ok {
					return nil, errors.New("not an int64")
				}
				buff = common.AppendUint64ToBufferLE(buff, uint64(i64val))
			case common.TypeDouble:
				f64Val, ok := arg.(float64)
				if !ok {
					return nil, errors.New("not an float64")
				}
				buff = common.AppendFloat64ToBufferLE(buff, f64Val)
			case common.TypeVarchar, common.TypeDecimal, common.TypeTimestamp:
				str, ok := arg.(string)
				if !ok {
					return nil, errors.New("not a string")
				}
				buff = common.AppendStringToBufferLE(buff, str)
			default:
				panic(fmt.Sprintf("unexpected arg type %d", argType.Type))
			}
		}
	}
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
	offset++
	q.PreparedStatement = buff[offset] == 1
	offset++
	if q.PreparedStatement {
		var numArgs uint32
		numArgs, offset = common.ReadUint32FromBufferLE(buff, offset)
		q.PsArgTypes = make([]common.ColumnType, numArgs)
		q.PsArgs = make([]interface{}, numArgs)
		for i := 0; i < int(numArgs); i++ {
			at := common.Type(buff[offset])
			offset++
			var argType common.ColumnType
			switch at {
			case common.TypeTinyInt:
				argType = common.TinyIntColumnType
			case common.TypeInt:
				argType = common.IntColumnType
			case common.TypeBigInt:
				argType = common.BigIntColumnType
			case common.TypeDouble:
				argType = common.DoubleColumnType
			case common.TypeVarchar:
				argType = common.VarcharColumnType
			case common.TypeDecimal:
				argType = common.ColumnType{Type: common.TypeDecimal}
				argType.DecPrecision = int(buff[offset])
				offset++
				argType.DecScale = int(buff[offset])
				offset++
			case common.TypeTimestamp:
				argType = common.ColumnType{Type: common.TypeTimestamp}
				argType.FSP = int8(buff[offset])
				offset++
			default:
				panic(fmt.Sprintf("unexpected arg type %d", at))
			}
			q.PsArgTypes[i] = argType
		}
		for i, argType := range q.PsArgTypes {
			var arg interface{}
			switch argType.Type {
			case common.TypeTinyInt, common.TypeInt, common.TypeBigInt:
				arg, offset = common.ReadUint64FromBufferLE(buff, offset)
			case common.TypeDouble:
				arg, offset = common.ReadFloat64FromBufferLE(buff, offset)
			case common.TypeVarchar, common.TypeDecimal, common.TypeTimestamp:
				arg, offset = common.ReadStringFromBufferLE(buff, offset)
			default:
				panic(fmt.Sprintf("unexpected arg type %d", argType.Type))
			}
			q.PsArgs[i] = arg
		}
	}
	return nil
}

func appendBool(buff []byte, val bool) []byte {
	var b byte
	if val {
		b = 1
	} else {
		b = 0
	}
	return append(buff, b)
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

type ForwardRow struct {
	ReceiverSequence uint64
	RemoteConsumerID uint64
	KeyBytes         []byte
	RowBytes         []byte
	WriteTime        uint64
}

type ShardListener interface {
	RemoteWriteOccurred(forwardRows []ForwardRow)
}
