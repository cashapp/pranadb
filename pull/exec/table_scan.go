package exec

import (
	"fmt"
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

func NewPullTableScan(tableInfo *common.TableInfo, storage cluster.Cluster, shardID uint64) *PullTableScan {
	rf := common.NewRowsFactory(tableInfo.ColumnTypes)
	base := pullExecutorBase{
		colTypes:    tableInfo.ColumnTypes,
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
	if limit == 0 || limit < -1 {
		return nil, fmt.Errorf("invalid limit %d", limit)
	}

	var tableShardPrefix = make([]byte, 0, 8)
	tableShardPrefix = common.AppendUint64ToBufferLittleEndian(tableShardPrefix, t.tableInfo.ID)
	tableShardPrefix = common.AppendUint64ToBufferLittleEndian(tableShardPrefix, t.shardID)

	var skipFirst bool
	var startPrefix []byte
	if t.lastRowPrefix == nil {
		startPrefix = tableShardPrefix
	} else {
		startPrefix = t.lastRowPrefix
		skipFirst = true
	}

	limitToUse := limit
	if limit != -1 && skipFirst {
		// We read one extra row as we'll skip the first
		limitToUse++
	}
	kvPairs, err := t.storage.LocalScan(startPrefix, tableShardPrefix, limitToUse)
	if err != nil {
		return nil, err
	}
	numRows := len(kvPairs)
	log.Printf("Got %d rows from table %d and shard %d", numRows, t.tableInfo.ID, t.shardID)
	rows = t.rowsFactory.NewRows(numRows)
	for i, kvPair := range kvPairs {
		log.Printf("Read row with key %v from table", kvPair.Key)
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
