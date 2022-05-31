package exec

import (
	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/table"
)

type PullIndexReader struct {
	pullExecutorBase
	tableInfo     *common.TableInfo
	indexInfo     *common.IndexInfo
	storage       cluster.Cluster
	shardID       uint64
	lastRowPrefix []byte
	rangeStart    []byte
	rangeEnd      []byte
	includeCols   []int
}

var _ PullExecutor = &PullIndexReader{}

/*
No hidden cols
*/

func NewPullIndexReader(tableInfo *common.TableInfo,
	indexInfo *common.IndexInfo,
	includedCols []int, // Col indexes in the table that are being returned
	storage cluster.Cluster,
	shardID uint64,
	scanRanges []*ScanRange) (*PullIndexReader, error) {

	// Calculate the types of the results
	var resultColTypes []common.ColumnType
	for _, colIndex := range includedCols {
		resultColTypes = append(resultColTypes, tableInfo.ColumnTypes[colIndex])
	}
	var resultColNames []string
	for _, colIndex := range includedCols {
		resultColNames = append(resultColNames, tableInfo.ColumnNames[colIndex])
	}

	rf := common.NewRowsFactory(resultColTypes)
	base := pullExecutorBase{
		colNames:    resultColNames,
		colTypes:    resultColTypes,
		rowsFactory: rf,
		keyCols:     tableInfo.PrimaryKeyCols,
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
		rangeStart = append(rangeStart, indexShardPrefix...)
		rangeEnd = append(rangeEnd, indexShardPrefix...)
		for i, scanRange := range scanRanges {
			// if range vals are nil, doing a point get with nil as the index PK value
			if scanRange.LowVal == nil {
				rangeStart = append(rangeStart, 0)
			} else {
				rangeStart = append(rangeStart, 1)
				rangeStart, err = common.EncodeKeyElement(scanRange.LowVal, tableInfo.ColumnTypes[indexInfo.IndexCols[i]], rangeStart)
				if err != nil {
					return nil, err
				}
				if scanRange.LowExcl {
					rangeStart = common.IncrementBytesBigEndian(rangeStart)
				}
			}
			if scanRange.HighVal == nil {
				rangeEnd = append(rangeEnd, 0)
			} else {
				rangeEnd = append(rangeEnd, 1)
				rangeEnd, err = common.EncodeKeyElement(scanRange.HighVal, tableInfo.ColumnTypes[indexInfo.IndexCols[i]], rangeEnd)
				if err != nil {
					return nil, err
				}

				if err != nil {
					return nil, err
				}
			}
			if !scanRange.HighExcl {
				if !allBitsSet(rangeEnd) {
					rangeEnd = common.IncrementBytesBigEndian(rangeEnd)
				} else {
					rangeEnd = nil
				}
			}
		}
	}

	if rangeStart == nil {
		rangeStart = indexShardPrefix
	}
	if rangeEnd == nil {
		rangeEnd = table.EncodeTableKeyPrefix(indexInfo.ID+1, shardID, 16)
	}
	return &PullIndexReader{
		pullExecutorBase: base,
		tableInfo:        tableInfo,
		indexInfo:        indexInfo,
		storage:          storage,
		shardID:          shardID,
		rangeStart:       rangeStart,
		rangeEnd:         rangeEnd,
		includeCols:      includedCols,
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
	// Need to add PK cols if selected
	var includedPkCols []int
	for _, keyCol := range p.keyCols {
		if common.Contains(p.includeCols, keyCol) {
			includedPkCols = append(includedPkCols, keyCol)
		}
	}
	// Check if index covers
	var nonIndexCols []int
	for _, keyCol := range p.includeCols {
		if !common.Contains(includedPkCols, keyCol) && !common.Contains(p.indexInfo.IndexCols, keyCol) {
			nonIndexCols = append(nonIndexCols, keyCol)
		}
	}
	var includeCol []bool
	for i := range p.colTypes {
		included := common.Contains(p.includeCols, i)
		includeCol = append(includeCol, included)
	}
	for i, kvPair := range kvPairs {
		if i == 0 && skipFirst {
			continue
		}
		if i == numRows-1 {
			p.lastRowPrefix = kvPair.Key
		}
		// Index covers if all cols are in the index
		if len(nonIndexCols) == 0 {
			offset := 16
			offset, err := common.DecodeIndexKeyWithIgnoredCols(kvPair.Key, offset, p.tableInfo.ColumnTypes, p.includeCols, p.indexInfo.IndexCols, len(includedPkCols), rows)

			for _, pkCol := range includedPkCols {
				colType := p.tableInfo.ColumnTypes[pkCol]
				_, err = common.DecodeIndexKeyCol(kvPair.Key, offset, colType, true, pkCol, true, rows)
			}
			if err != nil {
				return nil, errors.WithStack(err)
			}
			// Index doesn't cover, and we get cols from originating table
		} else {
			keyBuff := table.EncodeTableKeyPrefix(p.tableInfo.ID, p.shardID, 16+len(kvPair.Value))
			keyBuff = append(keyBuff, kvPair.Value...)
			value, err := p.storage.LocalGet(keyBuff)
			if err != nil {
				return nil, errors.WithStack(err)
			}
			if err = common.DecodeRowWithIgnoredCols(value, p.tableInfo.ColumnTypes, includeCol, rows); err != nil {
				return nil, errors.WithStack(err)
			}
		}
	}
	return rows, nil
}
