package exec

import (
	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/errors"
)

type PullTableScan struct {
	pullExecutorBase
	tableInfo     *common.TableInfo
	storage       cluster.Cluster
	shardID       uint64
	lastRowPrefix []byte
	rangeHolders  []*rangeHolder
	includeCols   []bool
	rangeIndex    int
	rows          *common.Rows
}

var _ PullExecutor = &PullTableScan{}

type ScanRange struct {
	LowVals  []interface{}
	HighVals []interface{}
	LowExcl  bool
	HighExcl bool
}

func NewPullTableScan(tableInfo *common.TableInfo, colIndexes []int, storage cluster.Cluster, shardID uint64,
	scanRanges []*ScanRange) (*PullTableScan, error) {

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

	rangeHolders, err := calcScanRangeKeys(scanRanges, tableInfo.ID, tableInfo.PrimaryKeyCols, tableInfo, shardID, false)
	if err != nil {
		return nil, err
	}
	return &PullTableScan{
		pullExecutorBase: base,
		tableInfo:        tableInfo,
		storage:          storage,
		shardID:          shardID,
		rangeHolders:     rangeHolders,
		includeCols:      includedCols,
	}, nil
}

type rangeHolder struct {
	rangeStart []byte
	rangeEnd   []byte
}

func (p *PullTableScan) getRowsFromRange(limit int, currRange *rangeHolder) error {

	var skipFirst bool
	var startPrefix []byte
	if p.lastRowPrefix == nil {
		startPrefix = currRange.rangeStart
	} else {
		startPrefix = p.lastRowPrefix
		skipFirst = true
	}

	rc := 0
	if p.rows != nil {
		rc = p.rows.RowCount()
	}
	limitToUse := limit - rc
	if skipFirst {
		// We read one extra row as we'll skip the first
		limitToUse++
	}

	kvPairs, err := p.storage.LocalScan(startPrefix, currRange.rangeEnd, limitToUse)
	if err != nil {
		return errors.WithStack(err)
	}
	numRows := len(kvPairs)
	if p.rows == nil {
		p.rows = p.rowsFactory.NewRows(numRows)
	}
	for i, kvPair := range kvPairs {
		if i == 0 && skipFirst {
			continue
		}
		if i == numRows-1 {
			p.lastRowPrefix = kvPair.Key
		}
		if err := common.DecodeRowWithIgnoredCols(kvPair.Value, p.tableInfo.ColumnTypes, p.includeCols, p.rows); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func (p *PullTableScan) GetRows(limit int) (rows *common.Rows, err error) {
	if limit < 1 {
		return nil, errors.Errorf("invalid limit %d", limit)
	}
	for p.rangeIndex < len(p.rangeHolders) {
		rng := p.rangeHolders[p.rangeIndex]
		if err := p.getRowsFromRange(limit, rng); err != nil {
			return nil, err
		}
		if p.rows.RowCount() == limit {
			break
		}
		p.rangeIndex++
		p.lastRowPrefix = nil
	}
	rows = p.rows
	p.rows = nil
	if rows == nil {
		rows = p.rowsFactory.NewRows(0)
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
