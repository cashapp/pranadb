package exec

import (
	"github.com/squareup/pranadb/common"
	"sync/atomic"
)

type UnionAll struct {
	pushExecutorBase
	idSequence    int64
	keyBase       []byte
	genIDColIndex int
}

func NewUnionAll(idSequenceBase int64) (*UnionAll, error) {
	keyBase := make([]byte, 0, 8)
	keyBase = common.AppendUint64ToBufferBE(keyBase, uint64(idSequenceBase))
	return &UnionAll{
		pushExecutorBase: pushExecutorBase{},
		keyBase:          keyBase,
	}, nil
}

func (t *UnionAll) ReCalcSchemaFromChildren() error {
	child0 := t.children[0]

	// For a UNION ALL we take rows from all inputs even if keys are same, so we can't use key of children as one row might overwrite the other
	// We therefore generate an internal id from a sequence and add this at the end
	t.colTypes = child0.ColTypes()
	t.colsVisible = child0.ColsVisible()

	t.genIDColIndex = len(child0.ColTypes())

	t.colTypes = append(t.colTypes, common.VarcharColumnType)
	t.colsVisible = append(t.colsVisible, false)

	t.keyCols = []int{t.genIDColIndex}
	t.rowsFactory = common.NewRowsFactory(t.colTypes)

	return nil
}

func (t *UnionAll) generateID(shardID uint64) string {
	// It doesn't matter that generated IDs for shards are contiguous but its best they are monotonically increasing
	// so the natural select order roughly reflects insertion time
	// We grab a cluster wide id sequence when we create the executor and use that as the first 8 bytes of the key
	// then for the next 8 bytes and then we have an in-memory counter.
	// We prepend the key with shardID so it's unique across all shards
	key := make([]byte, 0, 24)
	key = common.AppendUint64ToBufferBE(key, shardID)
	key = append(key, t.keyBase...)
	id := atomic.AddInt64(&t.idSequence, 1)
	key = common.AppendUint64ToBufferBE(key, uint64(id))
	return string(key)
}

func (t *UnionAll) HandleRows(rowsBatch RowsBatch, ctx *ExecutionContext) error {
	numRows := rowsBatch.Len()
	out := t.rowsFactory.NewRows(numRows)
	// TODO this could be optimised by just taking the columns from the incoming chunk and adding them to the new rows
	for i := 0; i < numRows; i++ {
		inRow := rowsBatch.CurrentRow(i)
		for i := 0; i < len(t.colTypes)-1; i++ {
			colType := t.colTypes[i]
			switch colType.Type {
			case common.TypeTinyInt, common.TypeInt, common.TypeBigInt:
				out.AppendInt64ToColumn(i, inRow.GetInt64(i))
			case common.TypeDouble:
				out.AppendFloat64ToColumn(i, inRow.GetFloat64(i))
			case common.TypeVarchar:
				out.AppendStringToColumn(i, inRow.GetString(i))
			case common.TypeTimestamp:
				out.AppendTimestampToColumn(i, inRow.GetTimestamp(i))
			case common.TypeDecimal:
				out.AppendDecimalToColumn(i, inRow.GetDecimal(i))
			default:
				panic("unexpected column type")
			}
		}
		// TODO if the child is an aggregation and can output modifies and deletes then generating an id won't work
		// as won't identify the previous row
		id := t.generateID(ctx.WriteBatch.ShardID)
		out.AppendStringToColumn(t.genIDColIndex, id)
	}
	return t.parent.HandleRows(NewCurrentRowsBatch(out), ctx)
}
