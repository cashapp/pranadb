package exec

import (
	"sync"

	"github.com/squareup/pranadb/cluster"

	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/table"
)

// TableExecutor updates the changes into the associated table - used to persist state
// of a materialized view or source
type TableExecutor struct {
	pushExecutorBase
	tableInfo      *common.TableInfo
	consumingNodes map[PushExecutor]struct{}
	store          cluster.Cluster
	lock           sync.RWMutex
}

func NewTableExecutor(colTypes []common.ColumnType, tableInfo *common.TableInfo, store cluster.Cluster) *TableExecutor {
	rf := common.NewRowsFactory(colTypes)
	pushBase := pushExecutorBase{
		colTypes:    colTypes,
		rowsFactory: rf,
	}
	return &TableExecutor{
		pushExecutorBase: pushBase,
		tableInfo:        tableInfo,
		store:            store,
		consumingNodes:   make(map[PushExecutor]struct{}),
	}
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
	t.lock.Lock()
	defer t.lock.Unlock()
	t.consumingNodes[node] = struct{}{}
}

func (t *TableExecutor) RemoveConsumingNode(node PushExecutor) {
	t.lock.Lock()
	defer t.lock.Unlock()
	delete(t.consumingNodes, node)
}

func (t *TableExecutor) HandleRemoteRows(rows *common.Rows, ctx *ExecutionContext) error {
	return t.HandleRows(rows, ctx)
}

func (t *TableExecutor) HandleRows(rows *common.Rows, ctx *ExecutionContext) error {
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
	t.lock.Lock()
	defer t.lock.Unlock()
	for consumingNode := range t.consumingNodes {
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
