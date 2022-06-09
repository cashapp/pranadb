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

	var err error
	indexShardPrefix := table.EncodeTableKeyPrefix(indexInfo.ID, shardID, 16)

	// The index key in Pebble is:
	// |shard_id|index_id|index_col0|index_col1|index_col2|...|table_pk_value
	//
	// When scanning the index in a range we create a range start and a range end that are a prefix of the index key
	// The key start, end, don't have to include all, or any, of the index columns
	var rangeStart, rangeEnd []byte
	if scanRanges != nil {
		if len(scanRanges) > 1 {
			return nil, errors.New("multiple scan ranges not supported")
		}
		sr := scanRanges[0]
		rangeStart = append(rangeStart, indexShardPrefix...)
		rangeEnd = append(rangeEnd, indexShardPrefix...)
		for i := 0; i < len(sr.LowVals); i++ {
			lv := sr.LowVals[i]
			hv := sr.HighVals[i]
			// if range vals are nil, doing a point get with nil as the index PK value
			if lv == nil && hv == nil {
				rangeStart = append(rangeStart, 0)
				rangeEnd = append(rangeEnd, 0)
			} else if hv == nil {
				rangeEnd = table.EncodeTableKeyPrefix(indexInfo.ID+1, shardID, 16)
			} else {
				rangeEnd = append(rangeEnd, 1)
				rangeEnd, err = common.EncodeKeyElement(hv, tableInfo.ColumnTypes[indexInfo.IndexCols[i]], rangeEnd)
				if err != nil {
					return nil, err
				}
			}
			if lv != nil {
				rangeStart = append(rangeStart, 1)
				rangeStart, err = common.EncodeKeyElement(lv, tableInfo.ColumnTypes[indexInfo.IndexCols[i]], rangeStart)
				if err != nil {
					return nil, err
				}
			}
		}
		if sr.LowExcl {
			rangeStart = common.IncrementBytesBigEndian(rangeStart)
		}
		if !sr.HighExcl {
			if !allBitsSet(rangeEnd) {
				rangeEnd = common.IncrementBytesBigEndian(rangeEnd)
			} else {
				rangeEnd = nil
			}
		}
	}

	if rangeStart == nil {
		rangeStart = indexShardPrefix
	}
	if rangeEnd == nil {
		rangeEnd = table.EncodeTableKeyPrefix(indexInfo.ID+1, shardID, 16)
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
