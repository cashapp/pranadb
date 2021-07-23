package exec

import (
	"bytes"
	"fmt"
	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/common"
	"log"
	"math"
)

type PullTableScan struct {
	pullExecutorBase
	tableInfo        *common.TableInfo
	storage          cluster.Cluster
	shardID          uint64
	lastRowPrefix    []byte
	rangeStart       []byte
	rangeEnd         []byte
	tableShardPrefix []byte
	lowExcl          bool
	highExcl         bool
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
	var lowExcl, highExcl bool
	var tableShardPrefix []byte
	tableShardPrefix = common.AppendUint64ToBufferLittleEndian(tableShardPrefix, tableInfo.ID)
	tableShardPrefix = common.AppendUint64ToBufferLittleEndian(tableShardPrefix, shardID)
	if scanRange != nil {
		// If a query contains a select (aka a filter, where clause) on a primary key this is often pushed down to the
		// table scan as a range
		if scanRange.LowVal != math.MinInt64 {
			rangeStart, err = common.EncodeKey([]interface{}{scanRange.LowVal}, tableInfo.ColumnTypes, tableInfo.PrimaryKeyCols, tableShardPrefix)
			if err != nil {
				return nil, err
			}
		}
		if scanRange.HighVal != math.MaxInt64 {
			rangeEnd, err = common.EncodeKey([]interface{}{scanRange.HighVal}, tableInfo.ColumnTypes, tableInfo.PrimaryKeyCols, tableShardPrefix)
			if err != nil {
				return nil, err
			}
		}
		lowExcl = scanRange.LowExcl
		highExcl = scanRange.HighExcl
	}
	if rangeStart == nil {
		rangeStart = tableShardPrefix
	}
	return &PullTableScan{
		pullExecutorBase: base,
		tableInfo:        tableInfo,
		storage:          storage,
		shardID:          shardID,
		rangeStart:       rangeStart,
		rangeEnd:         rangeEnd,
		lowExcl:          lowExcl,
		highExcl:         highExcl,
		tableShardPrefix: tableShardPrefix,
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
	var startPrefix, endPrefix []byte
	if t.lastRowPrefix == nil {
		startPrefix = t.rangeStart
	} else {
		startPrefix = t.lastRowPrefix
		skipFirst = true
	}
	if t.rangeEnd == nil {
		endPrefix = t.tableShardPrefix
	} else {
		endPrefix = t.rangeEnd
	}

	limitToUse := limit
	if limit != -1 && skipFirst {
		// We read one extra row as we'll skip the first
		limitToUse++
	}
	kvPairs, err := t.storage.LocalScan(startPrefix, endPrefix, limitToUse)
	if err != nil {
		return nil, err
	}
	log.Printf("Scanning from %v to %v returned %d rows", startPrefix, endPrefix, len(kvPairs))
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
		if t.lowExcl && bytes.Compare(t.rangeStart, kvPair.Key) == 0 {
			continue
		}
		if t.highExcl && bytes.Compare(t.rangeEnd, kvPair.Key) == 0 {
			continue
		}
		err := common.DecodeRow(kvPair.Value, t.tableInfo.ColumnTypes, rows)
		if err != nil {
			return nil, err
		}
	}
	return rows, nil
}
