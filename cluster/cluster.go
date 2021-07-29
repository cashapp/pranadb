package cluster

import (
	"fmt"
	"github.com/squareup/pranadb/common"
)

const (
	// SchemaTableShardID is a shard for storing table schemas. Note that this actually writes metadata to
	// the first data shard.
	SchemaTableShardID = 1000

	// DataShardIDBase is the lowest value of a data shard id
	DataShardIDBase uint64 = 1000
)

type Cluster interface {

	// WriteBatch writes a batch reliability to storage
	WriteBatch(batch *WriteBatch) error

	LocalGet(key []byte) ([]byte, error)

	// LocalScan scans the local store
	// endKeyPrefix is exclusive
	LocalScan(startKeyPrefix []byte, endKeyPrefix []byte, limit int) ([]KVPair, error)

	GetNodeID() int

	GetAllShardIDs() []uint64

	// GetLocalShardIDs returns the ids of the shards on the local node - this includes replicas
	GetLocalShardIDs() []uint64

	// GenerateTableID generates a table using a cluster wide persistent counter
	GenerateTableID() (uint64, error)

	SetRemoteQueryExecutionCallback(callback RemoteQueryExecutionCallback)

	RegisterShardListenerFactory(factory ShardListenerFactory)

	ExecuteRemotePullQuery(queryInfo *QueryExecutionInfo, rowsFactory *common.RowsFactory) (*common.Rows, error)

	// DeleteAllDataInRange deletes all data in the specified ranges. Ranges do not contain the shard id
	DeleteAllDataInRange(startPrefix []byte, endPrefix []byte) error

	RegisterMembershipListener(listener MembershipListener)

	Start() error

	Stop() error
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

func encodePsArgs(buff []byte, args []interface{}, argTypes []common.ColumnType) []byte {
	buff = common.AppendUint32ToBufferLittleEndian(buff, uint32(len(args)))
	for _, argType := range argTypes {
		buff = append(buff, byte(argType.Type))
	}
	for _, arg := range args {
		switch op := arg.(type) {
		case int64:
			buff = common.AppendUint64ToBufferLittleEndian(buff, uint64(op))
		case float64:
			buff = common.EncodeFloat64(op, buff)
		case string:
			buff = common.EncodeString(op, buff)
		case common.Decimal:
			// TODO
		default:
			panic(fmt.Sprintf("unexpected arg type %v", op))
		}
	}
	return buff
}

func decodePsArgs(offset int, buff []byte) ([]interface{}, int, error) {
	numArgs := common.ReadUint32FromBufferLittleEndian(buff, offset)
	offset += 4
	argTypes := make([]common.ColumnType, numArgs)
	for i := 0; i < int(numArgs); i++ {
		b := buff[offset]
		offset++
		t := common.Type(int(b))
		colType := common.ColumnType{Type: t}
		if colType.Type == common.TypeDecimal {
			colType.DecPrecision = int(common.ReadUint64FromBufferLittleEndian(buff, offset))
			offset += 8
			colType.DecScale = int(common.ReadUint64FromBufferLittleEndian(buff, offset))
			offset += 8
		}
		argTypes[i] = colType
	}

	args := make([]interface{}, numArgs)
	for i := 0; i < int(numArgs); i++ {
		argType := argTypes[i]
		switch argType.Type {
		case common.TypeTinyInt, common.TypeInt, common.TypeBigInt:
			args[i] = common.ReadUint64FromBufferLittleEndian(buff, offset)
			offset += 8
		case common.TypeDouble:
			args[i], offset = common.DecodeFloat64(buff, offset)
		case common.TypeVarchar:
			args[i], offset = common.DecodeString(buff, offset)
		case common.TypeDecimal:
			var err error
			args[i], offset, err = common.DecodeDecimal(buff, offset, argType.DecPrecision, argType.DecScale)
			if err != nil {
				return nil, 0, err
			}
		default:
			panic(fmt.Sprintf("unsupported col type %d", argType.Type))
		}
	}
	return args, offset, nil
}

func (q *QueryExecutionInfo) Serialize() []byte {
	var buff []byte
	buff = common.EncodeString(q.SessionID, buff)
	buff = common.EncodeString(q.SchemaName, buff)
	buff = common.EncodeString(q.Query, buff)
	buff = common.AppendUint64ToBufferLittleEndian(buff, uint64(q.PsID))
	buff = encodePsArgs(buff, q.PsArgs, q.PsArgTypes)
	buff = common.AppendUint32ToBufferLittleEndian(buff, q.Limit)
	buff = common.AppendUint64ToBufferLittleEndian(buff, q.ShardID)
	var b byte
	if q.IsPs {
		b = 1
	} else {
		b = 0
	}
	buff = append(buff, b)
	return buff
}

func (q *QueryExecutionInfo) Deserialize(buff []byte) error {
	offset := 0
	q.SessionID, offset = common.DecodeString(buff, offset)
	q.SchemaName, offset = common.DecodeString(buff, offset)
	q.Query, offset = common.DecodeString(buff, offset)
	q.PsID = int64(common.ReadUint64FromBufferLittleEndian(buff, offset))
	offset += 8
	var err error
	q.PsArgs, offset, err = decodePsArgs(offset, buff)
	if err != nil {
		return err
	}
	q.Limit = common.ReadUint32FromBufferLittleEndian(buff, offset)
	offset += 4
	q.ShardID = common.ReadUint64FromBufferLittleEndian(buff, offset)
	offset += 8
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
