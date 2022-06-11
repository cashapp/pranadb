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
	rangeStart        []byte
	rangeEnd          []byte
	covers            bool
	indexOutputCols   []int
	pkOutputCols      []int
	indexColTypes     []common.ColumnType
	pkColTypes        []common.ColumnType
	includedTableCols []bool
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
	var indexOutputCols, pkOutputCols []int
	var indexColTypes, pkColTypes []common.ColumnType
	var includedCols []bool
	if covers {
		// For each index column we calculate the position in the output or -1 if it doesn't appear in the output
		indexOutputCols = make([]int, len(indexInfo.IndexCols))
		for i, indexCol := range indexInfo.IndexCols {
			position, ok := colIndexesMap[indexCol]
			if ok {
				// The index col is in the output
				indexOutputCols[i] = position
			} else {
				indexOutputCols[i] = -1
			}
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
	if len(rangeHolders) > 1 {
		return nil, errors.Error("multiple ranges not supported")
	}
	rangeStart := rangeHolders[0].rangeStart
	rangeEnd := rangeHolders[0].rangeEnd

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
		rangeStart:        rangeStart,
		rangeEnd:          rangeEnd,
		covers:            covers,
		indexOutputCols:   indexOutputCols,
		pkOutputCols:      pkOutputCols,
		indexColTypes:     indexColTypes,
		pkColTypes:        pkColTypes,
		includedTableCols: includedCols,
	}, nil
}

func (p *PullIndexReader) GetRows(limit int) (rows *common.Rows, err error) { //nolint:gocyclo
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
		if p.covers {
			// Decode cols from the index
			if _, err = common.DecodeIndexOrPKCols(kvPair.Key, 16, false, p.indexColTypes, p.indexOutputCols, rows); err != nil {
				return nil, err
			}
			// And any from the PK
			if _, err = common.DecodeIndexOrPKCols(kvPair.Value, 0, true, p.pkColTypes, p.pkOutputCols, rows); err != nil {
				return nil, err
			}
		} else {
			// Index doesn't cover, and we get cols from originating table
			keyBuff := table.EncodeTableKeyPrefix(p.tableInfo.ID, p.shardID, 16+len(kvPair.Value))
			keyBuff = append(keyBuff, kvPair.Value...)
			value, err := p.storage.LocalGet(keyBuff)
			if err != nil {
				return nil, errors.WithStack(err)
			}
			if err = common.DecodeRowWithIgnoredCols(value, p.tableInfo.ColumnTypes, p.includedTableCols, rows); err != nil {
				return nil, errors.WithStack(err)
			}
		}
	}
	return rows, nil
}
