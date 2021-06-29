package exec

import (
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/table"
)

type PullTableScan struct {
	pullExecutorBase
	table table.Table
}

func NewPullTableScan(colTypes []common.ColumnType, table table.Table) (*PullTableScan, error) {
	base := pullExecutorBase{
		colTypes: colTypes,
	}
	return &PullTableScan{
		pullExecutorBase: base,
		table:            table,
	}, nil
}

func (t *PullTableScan) SetSchema(colNames []string, colTypes []common.ColumnType, keyCols []int) {
	t.colNames = colNames
	t.colTypes = colTypes
	t.keyCols = keyCols
}

func (t *PullTableScan) GetRows(limit int) (rows *common.Rows, err error) {
	// Do we even need to go through table for this?
	return t.table.LocalNodeTableScan(limit)
}
