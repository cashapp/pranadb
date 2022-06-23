package exec

import (
	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/table"
)

type PullIndexReader struct {
	pullExecutorBase
	tableInfo         *common.TableInfo
	indexInfo         *common.IndexInfo
	storage           cluster.Cluster
	shardID           uint64
	lastRowPrefix     []byte
	covers            bool
	indexOutputCols   []int
	pkOutputCols      []int
	indexColTypes     []common.ColumnType
	pkColTypes        []common.ColumnType
	includedTableCols []bool
	rows              *common.Rows
	rangeIndex        int
	rangeHolders      []*rangeHolder
}

var _ PullExecutor = &PullIndexReader{}

func NewPullIndexReader(tableInfo *common.TableInfo, //nolint:gocyclo
	indexInfo *common.IndexInfo,
	colIndexes []int, // Col indexes in the table that are being returned
	storage cluster.Cluster,
	shardID uint64,
	scanRanges []*ScanRange) (*PullIndexReader, error) {

	covers := true
	for _, colIndex := range colIndexes {
		if !tableInfo.IsPrimaryKeyCol(colIndex) {
			if !indexInfo.ContainsColIndex(colIndex) {
				covers = false
				break
			}
		}
	}
	// Put the col indexes in a map for quick O(1) access
	colIndexesMap := make(map[int]int, len(colIndexes))
	for i, colIndex := range colIndexes {
		colIndexesMap[colIndex] = i
	}
	// And the PK cols
	pkSet := make(map[int]struct{}, len(tableInfo.PrimaryKeyCols))
	for _, pkCol := range tableInfo.PrimaryKeyCols {
		pkSet[pkCol] = struct{}{}
	}
	var indexOutputCols, pkOutputCols []int
	var indexColTypes, pkColTypes []common.ColumnType
	var includedCols []bool
	if covers {
		// For each index column we calculate the position in the output or -1 if it doesn't appear in the output
		indexOutputCols = make([]int, len(indexInfo.IndexCols))
		for i, indexCol := range indexInfo.IndexCols {
			position, ok := colIndexesMap[indexCol]
			if ok {
				// We only count the index col if it's on a PK col. The PK cols will be added in pkOutputCols
				// and we don't want to add a column both in indexOutputCols and pkOutputCols
				_, ok := pkSet[indexCol]
				if !ok {
					// The index col is in the output
					indexOutputCols[i] = position
					continue
				}
			}
			indexOutputCols[i] = -1
		}

		indexColTypes = make([]common.ColumnType, len(indexInfo.IndexCols))
		for i, indexCol := range indexInfo.IndexCols {
			indexColTypes[i] = tableInfo.ColumnTypes[indexCol]
		}

		// We do the same for PK cols
		pkOutputCols = make([]int, len(tableInfo.PrimaryKeyCols))
		for i, pkCol := range tableInfo.PrimaryKeyCols {
			position, ok := colIndexesMap[pkCol]
			if ok {
				// The index col is in the output
				pkOutputCols[i] = position
			} else {
				pkOutputCols[i] = -1
			}
		}

		pkColTypes = make([]common.ColumnType, len(tableInfo.PrimaryKeyCols))
		for i, pkCol := range tableInfo.PrimaryKeyCols {
			pkColTypes[i] = tableInfo.ColumnTypes[pkCol]
		}
	} else {
		// We create an array which tells us, for each column in the table, whether it's included in the output
		includedCols = make([]bool, len(tableInfo.ColumnTypes))
		for i := 0; i < len(tableInfo.ColumnTypes); i++ {
			_, ok := colIndexesMap[i]
			includedCols[i] = ok
		}
	}

	// Calculate the types of the results
	var resultColTypes []common.ColumnType
	for _, colIndex := range colIndexes {
		resultColTypes = append(resultColTypes, tableInfo.ColumnTypes[colIndex])
	}
	var resultColNames []string
	for _, colIndex := range colIndexes {
		resultColNames = append(resultColNames, tableInfo.ColumnNames[colIndex])
	}

	rangeHolders, err := calcScanRangeKeys(scanRanges, indexInfo.ID, indexInfo.IndexCols, tableInfo, shardID, true)
	if err != nil {
		return nil, err
	}

	rf := common.NewRowsFactory(resultColTypes)
	base := pullExecutorBase{
		colNames:    resultColNames,
		colTypes:    resultColTypes,
		rowsFactory: rf,
		keyCols:     tableInfo.PrimaryKeyCols,
	}
	return &PullIndexReader{
		pullExecutorBase:  base,
		tableInfo:         tableInfo,
		indexInfo:         indexInfo,
		storage:           storage,
		shardID:           shardID,
		covers:            covers,
		indexOutputCols:   indexOutputCols,
		pkOutputCols:      pkOutputCols,
		indexColTypes:     indexColTypes,
		pkColTypes:        pkColTypes,
		includedTableCols: includedCols,
		rangeHolders:      rangeHolders,
	}, nil
}

func (p *PullIndexReader) getRowsFromRange(limit int, currRange *rangeHolder) error {
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
		if p.covers {
			// Decode cols from the index
			if _, err = common.DecodeIndexOrPKCols(kvPair.Key, 16, false, p.indexColTypes, p.indexOutputCols, p.rows); err != nil {
				return err
			}
			// And any from the PK
			if _, err = common.DecodeIndexOrPKCols(kvPair.Value, 0, true, p.pkColTypes, p.pkOutputCols, p.rows); err != nil {
				return err
			}
		} else {
			// Index doesn't cover, and we get cols from originating table
			keyBuff := table.EncodeTableKeyPrefix(p.tableInfo.ID, p.shardID, 16+len(kvPair.Value))
			keyBuff = append(keyBuff, kvPair.Value...)
			value, err := p.storage.LocalGet(keyBuff)
			if err != nil {
				return errors.WithStack(err)
			}
			if err = common.DecodeRowWithIgnoredCols(value, p.tableInfo.ColumnTypes, p.includedTableCols, p.rows); err != nil {
				return errors.WithStack(err)
			}
		}
	}
	return nil
}

func (p *PullIndexReader) GetRows(limit int) (rows *common.Rows, err error) {
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
