package exec

import (
	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/common"
	"log"
)

type PullTableScan struct {
	pullExecutorBase
	tableInfo     *common.TableInfo
	storage       cluster.Cluster
	shardID       uint64
	lastRowPrefix []byte
}

var _ PullExecutor = &PullTableScan{}

func NewPullTableScan(colTypes []common.ColumnType, tableInfo *common.TableInfo, storage cluster.Cluster, shardID uint64) *PullTableScan {
	rf := common.NewRowsFactory(colTypes)
	base := pullExecutorBase{
		colTypes:    colTypes,
		rowsFactory: rf,
	}
	return &PullTableScan{
		pullExecutorBase: base,
		tableInfo:        tableInfo,
		storage:          storage,
		shardID:          shardID,
	}
}

func (t *PullTableScan) SetSchema(colNames []string, colTypes []common.ColumnType, keyCols []int) {
	t.colNames = colNames
	t.colTypes = colTypes
	t.keyCols = keyCols
}

func (t *PullTableScan) GetRows(limit int) (rows *common.Rows, err error) {

	var prefix []byte
	var skipFirst bool
	if t.lastRowPrefix == nil {
		prefix = make([]byte, 0, 8)
		prefix = common.AppendUint64ToBufferLittleEndian(prefix, t.tableInfo.ID)
		prefix = common.AppendUint64ToBufferLittleEndian(prefix, t.shardID)
	} else {
		prefix = t.lastRowPrefix
		skipFirst = true
	}

	kvPairs, err := t.storage.LocalScan(prefix, prefix, limit)
	if err != nil {
		return nil, err
	}
	numRows := len(kvPairs)
	log.Printf("Got %d rows from table %d and shard %d", numRows, t.tableInfo.ID, t.shardID)
	rows = t.rowsFactory.NewRows(numRows)
	for i, kvPair := range kvPairs {
		if i == 0 && skipFirst {
			continue
		}
		if i == numRows-1 {
			t.lastRowPrefix = kvPair.Key
		}
		err := common.DecodeRow(kvPair.Value, t.tableInfo.ColumnTypes, rows)
		if err != nil {
			return nil, err
		}
	}
	return rows, nil
}
