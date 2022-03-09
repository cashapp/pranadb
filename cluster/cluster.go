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

	// WriteBatch writes a batch reliability to storage
	WriteBatch(batch *WriteBatch) error

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

	DeleteAllDataInRangeForAllShards(startPrefix []byte, endPrefix []byte) error

	DeleteAllDataInRangeForShardLocally(shardID uint64, startPrefix []byte, endPrefix []byte) error

	RegisterMembershipListener(listener MembershipListener)

	GetLock(prefix string) (bool, error)

	ReleaseLock(prefix string) (bool, error)

	// AddPrefixesToDelete Adds one or more key prefixes to delete from local storage at start-up
	AddPrefixesToDelete(local bool, prefixes ...[]byte) error

	RemovePrefixesToDelete(local bool, prefixes ...[]byte) error

	Start() error

	Stop() error
}

type Snapshot interface {
	Close()
}

type QueryExecutionInfo struct {
	SessionID  string
	SchemaName string
	Query      string
	PsID       int64
	PsArgs     []interface{}
	PsArgTypes []common.ColumnType
	Limit      uint32
	ShardID    uint64
	IsPs       bool
}

func (q *QueryExecutionInfo) GetArgs() []interface{} {
	return q.PsArgs
}

func encodePsArgs(buff []byte, args []interface{}, argTypes []common.ColumnType) ([]byte, error) {
	buff = common.AppendUint32ToBufferLE(buff, uint32(len(args)))
	for _, argType := range argTypes {
		buff = append(buff, byte(argType.Type))
	}
	var err error
	for i, arg := range args {
		argType := argTypes[i]
		switch arg := arg.(type) {
		case int64:
			buff = common.AppendUint64ToBufferLE(buff, uint64(arg))
		case float64:
			buff = common.AppendFloat64ToBufferLE(buff, arg)
		case string:
			buff = common.AppendStringToBufferLE(buff, arg)
		case common.Decimal:
			buff, err = common.AppendDecimalToBuffer(buff, arg, argType.DecPrecision, argType.DecScale)
			if err != nil {
				return nil, errors.WithStack(err)
			}
		case common.Timestamp:
			buff, err = common.AppendTimestampToBuffer(buff, arg)
			if err != nil {
				return nil, errors.WithStack(err)
			}
		default:
			panic(fmt.Sprintf("unexpected arg type %v", arg))
		}
	}
	return buff, errors.WithStack(err)
}

func decodePsArgs(offset int, buff []byte) ([]interface{}, int, error) {
	numArgs, offset := common.ReadUint32FromBufferLE(buff, offset)
	argTypes := make([]common.ColumnType, numArgs)
	for i := 0; i < int(numArgs); i++ {
		b := buff[offset]
		offset++
		t := common.Type(int(b))
		colType := common.ColumnType{Type: t}
		if colType.Type == common.TypeDecimal {
			var dp uint64
			dp, offset = common.ReadUint64FromBufferLE(buff, offset)
			colType.DecPrecision = int(dp)
			var dc uint64
			dc, offset = common.ReadUint64FromBufferLE(buff, offset)
			colType.DecScale = int(dc)
		}
		argTypes[i] = colType
	}

	args := make([]interface{}, numArgs)
	var err error
	for i := 0; i < int(numArgs); i++ {
		argType := argTypes[i]
		switch argType.Type {
		case common.TypeTinyInt, common.TypeInt, common.TypeBigInt:
			args[i], offset = common.ReadUint64FromBufferLE(buff, offset)
		case common.TypeDouble:
			args[i], offset = common.ReadFloat64FromBufferLE(buff, offset)
		case common.TypeVarchar:
			args[i], offset = common.ReadStringFromBufferLE(buff, offset)
		case common.TypeDecimal:
			args[i], offset, err = common.ReadDecimalFromBuffer(buff, offset, argType.DecPrecision, argType.DecScale)
			if err != nil {
				return nil, 0, errors.WithStack(err)
			}
		case common.TypeTimestamp:
			args[i], offset, err = common.ReadTimestampFromBuffer(buff, offset, argType.FSP)
			if err != nil {
				return nil, 0, errors.WithStack(err)
			}
		default:
			panic(fmt.Sprintf("unsupported col type %d", argType.Type))
		}
	}
	return args, offset, nil
}

func (q *QueryExecutionInfo) Serialize(buff []byte) ([]byte, error) {
	buff = common.AppendStringToBufferLE(buff, q.SessionID)
	buff = common.AppendStringToBufferLE(buff, q.SchemaName)
	buff = common.AppendStringToBufferLE(buff, q.Query)
	buff = common.AppendUint64ToBufferLE(buff, uint64(q.PsID))
	buff, err := encodePsArgs(buff, q.PsArgs, q.PsArgTypes)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	buff = common.AppendUint32ToBufferLE(buff, q.Limit)
	buff = common.AppendUint64ToBufferLE(buff, q.ShardID)
	var b byte
	if q.IsPs {
		b = 1
	} else {
		b = 0
	}
	buff = append(buff, b)
	return buff, nil
}

func (q *QueryExecutionInfo) Deserialize(buff []byte) error {
	offset := 0
	q.SessionID, offset = common.ReadStringFromBufferLE(buff, offset)
	q.SchemaName, offset = common.ReadStringFromBufferLE(buff, offset)
	q.Query, offset = common.ReadStringFromBufferLE(buff, offset)
	q.PsID, offset = common.ReadInt64FromBufferLE(buff, offset)
	var err error
	q.PsArgs, offset, err = decodePsArgs(offset, buff)
	if err != nil {
		return errors.WithStack(err)
	}
	q.Limit, offset = common.ReadUint32FromBufferLE(buff, offset)
	q.ShardID, offset = common.ReadUint64FromBufferLE(buff, offset)
	q.IsPs = buff[offset] == 1
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

type MembershipListener interface {
	NodeJoined(nodeID int)

	NodeLeft(nodeID int)
}
