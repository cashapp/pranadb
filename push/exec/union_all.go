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
	keyBase := make([]byte, 0, 16)
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
	t.colNames = child0.ColNames()
	t.colTypes = child0.ColTypes()
	t.colsVisible = child0.ColsVisible()

	t.genIDColIndex = len(child0.ColTypes())

	t.colNames = append(t.colNames, "__gen_id0")
	t.colTypes = append(t.colTypes, common.VarcharColumnType)
	t.colsVisible = append(t.colsVisible, false)

	t.keyCols = []int{t.genIDColIndex}
	t.rowsFactory = common.NewRowsFactory(t.colTypes)
	return nil
}

func (t *UnionAll) generateID() string {
	// It doesn't matter that generated IDs for shards are contiguous but its best they are monotonically increasing
	// so the natural select order roughly reflects insertion time
	// Also IDs don't have to globally unique across all shards - the IDS are never used exposed in user queries
	// only used for looking up rows in a particular shard, from an index in the same shard so they only need to be
	// unique per shard
	// So we grab a cluster wide id sequence when we create the executor and use that as the first 8 bytes of the key
	// then we have an in-memory counter for the next 8 bytes.
	id := atomic.AddInt64(&t.idSequence, 1)
	key := common.AppendUint64ToBufferBE(t.keyBase, uint64(id))
	return string(key)
}

func (t *UnionAll) HandleRows(incoming *common.Rows, ctx *ExecutionContext) error {
	in, err := maybeCastRows(incoming)
	if err != nil {
		return err
	}
	out := t.rowsFactory.NewRows(incoming.RowCount())
	// TODO this could be optimised by just taking the columns from the incoming chunk and adding them to the new rows
	for i := 0; i < incoming.RowCount(); i++ {
		inRow := in.GetRow(i)
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
		id := t.generateID()
		out.AppendStringToColumn(t.genIDColIndex, id)
	}
	return t.parent.HandleRows(out, ctx)
}

func maybeCastRows(incoming *common.Rows) (*common.Rows, error) {
	return incoming, nil
}
