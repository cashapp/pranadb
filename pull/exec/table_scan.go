package exec

import (
	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/table"
)

type PullTableScan struct {
	pullExecutorBase
	tableInfo     *common.TableInfo
	storage       cluster.Cluster
	shardID       uint64
	lastRowPrefix []byte
	rangeStart    []byte
	rangeEnd      []byte

	includeCols []bool
}

var _ PullExecutor = &PullTableScan{}

type ScanRange struct {
	LowVal   interface{}
	HighVal  interface{}
	LowExcl  bool
	HighExcl bool
}

func NewPullTableScan(tableInfo *common.TableInfo, colIndexes []int, storage cluster.Cluster, shardID uint64, scanRange *ScanRange) (*PullTableScan, error) {

	// The rows that we create for a pull query don't include hidden rows
	// Also, we don't always select all columns, depending on whether colIndexes has been specified
	var resultColTypes []common.ColumnType
	includedCols := make([]bool, len(tableInfo.ColumnTypes))
	ciMap := map[int]struct{}{}
	for _, colIndex := range colIndexes {
		ciMap[colIndex] = struct{}{}
	}
	for i := 0; i < len(tableInfo.ColumnTypes); i++ {
		_, ok := ciMap[i]
		includedCols[i] = (colIndexes == nil || ok) && (tableInfo.ColsVisible == nil || tableInfo.ColsVisible[i])
		if includedCols[i] {
			resultColTypes = append(resultColTypes, tableInfo.ColumnTypes[i])
		}
	}

	rf := common.NewRowsFactory(resultColTypes)
	base := pullExecutorBase{
		colTypes:    tableInfo.ColumnTypes,
		rowsFactory: rf,
		keyCols:     tableInfo.PrimaryKeyCols,
	}
	var rangeStart, rangeEnd []byte
	var err error
	tableShardPrefix := table.EncodeTableKeyPrefix(tableInfo.ID, shardID, 16)

	// If a query contains a select (aka a filter, where clause) on a primary key this is often pushed down to the
	// table scan as a range
	if scanRange != nil {
		rangeStart, err = common.EncodeKey([]interface{}{scanRange.LowVal}, tableInfo.ColumnTypes, tableInfo.PrimaryKeyCols, tableShardPrefix)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		if scanRange.LowExcl {
			rangeStart = common.IncrementBytesBigEndian(rangeStart)
		}
		rangeEnd, err = common.EncodeKey([]interface{}{scanRange.HighVal}, tableInfo.ColumnTypes, tableInfo.PrimaryKeyCols, tableShardPrefix)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		if !scanRange.HighExcl {
			if !allBitsSet(rangeEnd) {
				rangeEnd = common.IncrementBytesBigEndian(rangeEnd)
			} else {
				rangeEnd = nil
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
		includeCols:      includedCols,
	}, nil
}

func (p *PullTableScan) Reset() {
	p.lastRowPrefix = nil
	p.pullExecutorBase.Reset()
}

func (p *PullTableScan) GetRows(limit int) (rows *common.Rows, err error) {
	if limit < 1 {
		return nil, errors.Errorf("invalid limit %d", limit)
	}

	var skipFirst bool
	var startPrefix []byte
	if p.lastRowPrefix == nil {
		startPrefix = p.rangeStart
	} else {
		startPrefix = p.lastRowPrefix
		skipFirst = true
	}

	limitToUse := limit
	if limit != -1 && skipFirst {
		// We read one extra row as we'll skip the first
		limitToUse++
	}

	kvPairs, err := p.storage.LocalScan(startPrefix, p.rangeEnd, limitToUse)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	numRows := len(kvPairs)
	rows = p.rowsFactory.NewRows(numRows)
	for i, kvPair := range kvPairs {
		if i == 0 && skipFirst {
			continue
		}
		if i == numRows-1 {
			p.lastRowPrefix = kvPair.Key
		}
		if err := common.DecodeRowWithIgnoredCols(kvPair.Value, p.tableInfo.ColumnTypes, p.includeCols, rows); err != nil {
			return nil, errors.WithStack(err)
		}
	}
	return rows, nil
}

func allBitsSet(bytes []byte) bool {
	for _, b := range bytes {
		if b != 255 {
			return false
		}
	}
	return true
}
