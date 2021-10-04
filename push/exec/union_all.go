package exec

import (
	"github.com/squareup/pranadb/common"
)

type UnionAll struct {
	pushExecutorBase
	genIDColIndex int
}

func NewUnionAll() (*UnionAll, error) {
	return &UnionAll{
		pushExecutorBase: pushExecutorBase{},
	}, nil
}

func (t *UnionAll) ReCalcSchemaFromChildren() error {

	child0 := t.children[0]

	t.colTypes = child0.ColTypes()
	t.colsVisible = child0.ColsVisible()

	t.genIDColIndex = len(child0.ColTypes())

	t.colTypes = append(t.colTypes, common.VarcharColumnType)
	t.colsVisible = append(t.colsVisible, false)

	t.keyCols = []int{t.genIDColIndex}
	t.rowsFactory = common.NewRowsFactory(t.colTypes)

	// When a row is incoming we need to know which source it came from as we generate the new key from this, in order to
	// do this we create an intermediate object for each input which calls HandleRowsWithIndex passing in the index
	var newChildren []PushExecutor
	for i, exec := range t.children {
		forwarder := &RowWithIndexForwarder{
			pushExecutorBase: pushExecutorBase{},
			index:            i,
			unionAll:         t,
		}
		newChildren = append(newChildren, forwarder)
		exec.SetParent(forwarder)
		forwarder.SetParent(t)
		forwarder.AddChild(exec)
	}
	t.children = newChildren

	return nil
}

func (t *UnionAll) HandleRows(rowsBatch RowsBatch, ctx *ExecutionContext) error {
	panic("should not be called")
}

func (t *UnionAll) HandleRowsWithIndex(index int, rowsBatch RowsBatch, ctx *ExecutionContext) error {
	numRows := rowsBatch.Len()

	// TODO this could be optimised by just taking the columns from the incoming chunk and adding them to the new rows

	out := t.rowsFactory.NewRows(numRows)
	entries := make([]RowsEntry, numRows)
	rc := 0
	for i := 0; i < numRows; i++ {
		prevRow := rowsBatch.CurrentRow(i)
		pi := -1
		if prevRow != nil {
			if err := t.appendRow(index, prevRow, out); err != nil {
				return err
			}
			pi = rc
			rc++
		}
		currRow := rowsBatch.CurrentRow(i)
		ci := -1
		if currRow != nil {
			if err := t.appendRow(index, currRow, out); err != nil {
				return err
			}
			ci = rc
			rc++
		}
		entries[i] = NewRowsEntry(pi, ci)
	}
	return t.parent.HandleRows(NewRowsBatch(out, entries), ctx)
}

func (t *UnionAll) appendRow(index int, inRow *common.Row, out *common.Rows) error {
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
	id, err := t.generateID(inRow, index)
	if err != nil {
		return err
	}
	out.AppendStringToColumn(t.genIDColIndex, id)
	return nil
}

func (t *UnionAll) generateID(row *common.Row, index int) (string, error) {
	actualChild := t.children[index].GetChildren()[0]
	// We create the key by prefixing with the index, then appending the key from the actual child
	var key []byte
	key = common.AppendUint32ToBufferLE(key, uint32(index))
	key, err := common.EncodeKeyCols(row, actualChild.KeyCols(), actualChild.ColTypes(), key)
	if err != nil {
		return "", err
	}
	return string(key), nil
}

type RowWithIndexForwarder struct {
	pushExecutorBase
	index    int
	unionAll *UnionAll
}

func (r *RowWithIndexForwarder) HandleRows(rowsBatch RowsBatch, ctx *ExecutionContext) error {
	return r.unionAll.HandleRowsWithIndex(r.index, rowsBatch, ctx)
}
