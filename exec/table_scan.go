package exec

import (
	"github.com/squareup/pranadb/common"
)

type TableScan struct {
	pushExecutorBase
	TableName string
}

func NewTableScan(colTypes []common.ColumnType, tableName string) (*TableScan, error) {
	pushBase := pushExecutorBase{
		executorBase: executorBase{
			colTypes: colTypes,
		},
	}
	return &TableScan{
		pushExecutorBase: pushBase,
		TableName:        tableName,
	}, nil
}

func (t *TableScan) SetSchema(colNames []string, colTypes []common.ColumnType, keyCols []int) {
	t.colNames = colNames
	t.colTypes = colTypes
	t.keyCols = keyCols
}

func (t *TableScan) ReCalcSchemaFromChildren() {
	// NOOP
}

func (t *TableScan) HandleRows(rows *common.Rows, ctx *ExecutionContext) error {
	return t.parent.HandleRows(rows, ctx)
}
