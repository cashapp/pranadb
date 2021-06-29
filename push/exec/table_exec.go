package exec

import (
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/storage"
	"github.com/squareup/pranadb/table"
	"log"
)

// TableExecutor updates the changes into the associated table - used to persist state
// of a materialized view or source
type TableExecutor struct {
	pushExecutorBase
	tableInfo      *common.TableInfo
	consumingNodes []PushExecutor
	store          storage.Storage
}

func NewTableExecutor(colTypes []common.ColumnType, tableInfo *common.TableInfo, store storage.Storage) (*TableExecutor, error) {
	rf, err := common.NewRowsFactory(colTypes)
	if err != nil {
		return nil, err
	}
	pushBase := pushExecutorBase{
		colTypes:    colTypes,
		rowsFactory: rf,
	}
	return &TableExecutor{
		pushExecutorBase: pushBase,
		tableInfo:        tableInfo,
		store:            store,
	}, nil
}

func (t *TableExecutor) ReCalcSchemaFromChildren() {
	if len(t.children) > 1 {
		panic("too many children")
	}
	if len(t.children) == 1 {
		child := t.children[0]
		t.colNames = child.ColNames()
		t.colTypes = child.ColTypes()
		t.keyCols = child.KeyCols()
	}
}

func (t *TableExecutor) AddConsumingNode(node PushExecutor) {
	t.consumingNodes = append(t.consumingNodes, node)
}

func (t *TableExecutor) HandleRows(rows *common.Rows, ctx *ExecutionContext) error {
	log.Printf("Table executor writing %d rows into table state", rows.RowCount())
	for i := 0; i < rows.RowCount(); i++ {
		row := rows.GetRow(i)
		err := table.Upsert(t.tableInfo, &row, ctx.WriteBatch)
		if err != nil {
			return err
		}
	}
	return t.ForwardToConsumingNodes(rows, ctx)
}

func (t *TableExecutor) ForwardToConsumingNodes(rows *common.Rows, ctx *ExecutionContext) error {
	for _, consumingNode := range t.consumingNodes {
		err := consumingNode.HandleRows(rows, ctx)
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *TableExecutor) RowsFactory() *common.RowsFactory {
	return t.rowsFactory
}
