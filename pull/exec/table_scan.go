package exec

import (
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/storage"
	"github.com/squareup/pranadb/table"
)

type PullTableScan struct {
	pullExecutorBase
	tableInfo *common.TableInfo
	storage   storage.Storage
}

var _ PullExecutor = &PullTableScan{}

func NewPullTableScan(colTypes []common.ColumnType, tableInfo *common.TableInfo, storage storage.Storage) *PullTableScan {
	rf := common.NewRowsFactory(colTypes)
	base := pullExecutorBase{
		colTypes:    colTypes,
		rowsFactory: rf,
	}
	return &PullTableScan{
		pullExecutorBase: base,
		tableInfo:        tableInfo,
		storage:          storage,
	}
}

func (t *PullTableScan) SetSchema(colNames []string, colTypes []common.ColumnType, keyCols []int) {
	t.colNames = colNames
	t.colTypes = colTypes
	t.keyCols = keyCols
}

func (t *PullTableScan) GetRows(limit int) (rows *common.Rows, err error) {
	return table.LocalNodeTableScan(t.tableInfo, limit, t.rowsFactory, t.storage)
}
