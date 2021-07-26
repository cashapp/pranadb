package exec

import (
	"fmt"
	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/table"
	"math"
)

type PullTableScan struct {
	pullExecutorBase
	tableInfo     *common.TableInfo
	storage       cluster.Cluster
	shardID       uint64
	lastRowPrefix []byte
	rangeStart    []byte
	rangeEnd      []byte
}

var _ PullExecutor = &PullTableScan{}

type ScanRange struct {
	LowVal   int64
	HighVal  int64
	LowExcl  bool
	HighExcl bool
}

func NewPullTableScan(tableInfo *common.TableInfo, storage cluster.Cluster, shardID uint64, scanRange *ScanRange) (*PullTableScan, error) {
	rf := common.NewRowsFactory(tableInfo.ColumnTypes)
	base := pullExecutorBase{
		colTypes:    tableInfo.ColumnTypes,
		rowsFactory: rf,
		keyCols:     tableInfo.PrimaryKeyCols,
	}
	var rangeStart, rangeEnd []byte
	var err error
	tableShardPrefix := table.EncodeTableKeyPrefix(tableInfo.ID, shardID, 16)
	if scanRange != nil {
		// If a query contains a select (aka a filter, where clause) on a primary key this is often pushed down to the
		// table scan as a range
		if scanRange.LowVal != math.MinInt64 {
			lr := scanRange.LowVal
			if scanRange.LowExcl {
				lr++
			}
			rangeStart, err = common.EncodeKey([]interface{}{lr}, tableInfo.ColumnTypes, tableInfo.PrimaryKeyCols, tableShardPrefix)
			if err != nil {
				return nil, err
			}
		}
		if scanRange.HighVal != math.MaxInt64 {
			hr := scanRange.HighVal
			if !scanRange.HighExcl {
				hr++
			}
			rangeEnd, err = common.EncodeKey([]interface{}{hr}, tableInfo.ColumnTypes, tableInfo.PrimaryKeyCols, tableShardPrefix)
			if err != nil {
				return nil, err
			}
		}
	}
	if rangeStart == nil {
		rangeStart = tableShardPrefix
	}
	if rangeEnd == nil {
		rangeEnd = table.EncodeTableKeyPrefix(tableInfo.ID+1, shardID, 16)
	}
	return &PullTableScan{
		pullExecutorBase: base,
		tableInfo:        tableInfo,
		storage:          storage,
		shardID:          shardID,
		rangeStart:       rangeStart,
		rangeEnd:         rangeEnd,
	}, nil
}

func (t *PullTableScan) Reset() {
	t.lastRowPrefix = nil
	t.pullExecutorBase.Reset()
}

func (t *PullTableScan) GetRows(limit int) (rows *common.Rows, err error) {
	if limit == 0 || limit < -1 {
		return nil, fmt.Errorf("invalid limit %d", limit)
	}

	var skipFirst bool
	var startPrefix []byte
	if t.lastRowPrefix == nil {
		startPrefix = t.rangeStart
	} else {
		startPrefix = t.lastRowPrefix
		skipFirst = true
	}

	limitToUse := limit
	if limit != -1 && skipFirst {
		// We read one extra row as we'll skip the first
		limitToUse++
	}
	kvPairs, err := t.storage.LocalScan(startPrefix, t.rangeEnd, limitToUse)
	if err != nil {
		return nil, err
	}
	numRows := len(kvPairs)
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
