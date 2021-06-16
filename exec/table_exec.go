package exec

import (
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/storage"
)

// TableExecutor updates the changes into the associated table - used to persist state
// of a materialized view or source
type TableExecutor struct {
	pushExecutorBase
	table          common.Table
	consumingNodes []PushExecutor
	store          storage.Storage
}

func NewTableExecutor(colTypes []common.ColumnType, table common.Table, store storage.Storage) (*TableExecutor, error) {
	rf, err := common.NewRowsFactory(colTypes)
	if err != nil {
		return nil, err
	}
	base := pushExecutorBase{
		colTypes:    colTypes,
		rowsFactory: rf,
	}
	return &TableExecutor{
		pushExecutorBase: base,
		table:            table,
		store:            store,
	}, nil
}

func (t *TableExecutor) ReCalcSchema() {
	if len(t.children) > 1 {
		panic("too many children")
	}
	if len(t.children) == 1 {
		child := t.children[0]
		child.ReCalcSchema()
		t.ReCalcSchemaFromSources(child.ColNames(), child.ColTypes(), child.KeyCols())
	}
}

func (t *TableExecutor) ReCalcSchemaFromSources(colNames []string, colTypes []common.ColumnType, keyCols []int) {
	t.keyCols = keyCols
	t.colNames = colNames
	t.colTypes = colTypes
}

func (t *TableExecutor) AddConsumingNode(node PushExecutor) {
	t.consumingNodes = append(t.consumingNodes, node)
}

func (t *TableExecutor) HandleRows(rows *common.PushRows, ctx *ExecutionContext) error {
	for i := 0; i < rows.RowCount(); i++ {
		row := rows.GetRow(i)
		t.table.Upsert(&row, ctx.WriteBatch)
	}
	return t.ForwardToConsumingNodes(rows, ctx)
}

func (t *TableExecutor) ForwardToConsumingNodes(rows *common.PushRows, ctx *ExecutionContext) error {
	for _, consumingNode := range t.consumingNodes {
		err := consumingNode.HandleRows(rows, ctx)
		if err != nil {
			return err
		}
	}
	return nil
}
