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
	covers        bool
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
	scanRanges []*ScanRange,
	covers bool) (*PullIndexReader, error) {

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
		covers:           covers,
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
		// Need to add PK cols if selected
		var includedPkCols []int
		for _, keyCol := range p.keyCols {
			if common.Contains(p.includeCols, keyCol) {
				includedPkCols = append(includedPkCols, keyCol)
			}
		}
		_, offset := common.ReadUint64FromBufferBE(kvPair.Key, 0)
		_, offset = common.ReadUint64FromBufferBE(kvPair.Key, offset)
		offset, err := common.DecodeIndexKeyWithIgnoredCols(kvPair.Key, offset, p.tableInfo.ColumnTypes, p.includeCols, p.indexInfo.IndexCols, len(includedPkCols), rows)
		for _, pkCol := range includedPkCols {
			colType := p.tableInfo.ColumnTypes[pkCol]
			_, err = common.DecodeIndexKeyCol(kvPair.Key, offset, colType, true, pkCol, true, rows)
		}
		if err != nil {
			return nil, errors.WithStack(err)
		}
		if !p.covers {
			// Find cols selected that are not in the index
			var includeCol []bool
			includeColCount := 0
			for i := range p.colTypes {
				included := common.Contains(p.includeCols, i) && !common.Contains(p.indexInfo.IndexCols, i)
				includeCol = append(includeCol, included)
				if included {
					includeColCount++
				}
			}
			tableAddress := append(table.EncodeTableKeyPrefix(p.tableInfo.ID, p.shardID, 32), kvPair.Key[offset:]...)
			value, err := p.storage.LocalGet(tableAddress)
			if err != nil {
				return nil, errors.WithStack(err)
			}
			startCol := len(p.includeCols) - includeColCount
			offset := 0
			for i, colIncluded := range includeCol {
				offset, err = common.DecodeRowCol(value, offset, rows, p.tableInfo.ColumnTypes[i], startCol, colIncluded)
				if err != nil {
					return nil, errors.WithStack(err)
				}
				if colIncluded {
					startCol++
				}
			}

		}
	}
	return rows, nil
}
