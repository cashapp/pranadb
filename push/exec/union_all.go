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

func (u *UnionAll) ReCalcSchemaFromChildren() error {

	child0 := u.children[0]

	u.colTypes = child0.ColTypes()
	u.colsVisible = child0.ColsVisible()

	u.genIDColIndex = len(child0.ColTypes())

	u.colTypes = append(u.colTypes, common.VarcharColumnType)
	u.colsVisible = append(u.colsVisible, false)

	u.keyCols = []int{u.genIDColIndex}
	u.rowsFactory = common.NewRowsFactory(u.colTypes)

	// When a row is incoming we need to know which source it came from as we generate the new key from this, in order to
	// do this we create an intermediate object for each input which calls HandleRowsWithIndex passing in the index
	var newChildren []PushExecutor
	for i, exec := range u.children {
		forwarder := &RowWithIndexForwarder{
			pushExecutorBase: pushExecutorBase{},
			index:            i,
			unionAll:         u,
		}
		newChildren = append(newChildren, forwarder)
		exec.SetParent(forwarder)
		forwarder.SetParent(u)
		forwarder.AddChild(exec)
	}
	u.children = newChildren

	return nil
}

func (u *UnionAll) HandleRows(rowsBatch RowsBatch, ctx *ExecutionContext) error {
	panic("should not be called")
}

func (u *UnionAll) HandleRowsWithIndex(index int, rowsBatch RowsBatch, ctx *ExecutionContext) error {

	numRows := rowsBatch.Len()

	// TODO this could be optimised by just taking the columns from the incoming chunk and adding them to the new rows

	out := u.rowsFactory.NewRows(numRows)
	entries := make([]RowsEntry, numRows)
	rc := 0
	for i := 0; i < numRows; i++ {
		prevRow := rowsBatch.PreviousRow(i)
		pi := -1
		if prevRow != nil {
			if err := u.appendRow(index, prevRow, out); err != nil {
				return err
			}
			pi = rc
			rc++
		}
		currRow := rowsBatch.CurrentRow(i)
		ci := -1
		if currRow != nil {
			if err := u.appendRow(index, currRow, out); err != nil {
				return err
			}
			ci = rc
			rc++
		}
		entries[i] = NewRowsEntry(pi, ci)
	}

	return u.parent.HandleRows(NewRowsBatch(out, entries), ctx)
}

func (u *UnionAll) appendRow(index int, inRow *common.Row, out *common.Rows) error {
	for i := 0; i < len(u.colTypes)-1; i++ {
		colType := u.colTypes[i]
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
	id, err := u.generateID(inRow, index)
	if err != nil {
		return err
	}
	out.AppendStringToColumn(u.genIDColIndex, id)
	return nil
}

func (u *UnionAll) generateID(row *common.Row, index int) (string, error) {
	actualChild := u.children[index].GetChildren()[0]
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
