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
	storage       cluster.Cluster
	shardID       uint64
	lastRowPrefix []byte
	rangeStart    []byte
	rangeEnd      []byte
	includeCols   []int
	hasRange      bool
	covers        bool
	indexKeyLen   int
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

	for i, colIndex := range includedCols {
		resultColTypes[i] = tableInfo.ColumnTypes[colIndex]
	}

	rf := common.NewRowsFactory(resultColTypes)
	base := pullExecutorBase{
		colTypes:    tableInfo.ColumnTypes,
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
			rangeStart, err = common.EncodeKeyElement(scanRange.LowVal, tableInfo.ColumnTypes[indexInfo.IndexCols[i]], rangeStart)
			if err != nil {
				return nil, err
			}
			if scanRange.LowExcl {
				rangeStart = common.IncrementBytesBigEndian(rangeStart)
			}
			rangeEnd, err = common.EncodeKeyElement(scanRange.HighVal, tableInfo.ColumnTypes[indexInfo.IndexCols[i]], rangeEnd)
			if err != nil {
				return nil, err
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
		storage:          storage,
		shardID:          shardID,
		rangeStart:       rangeStart,
		rangeEnd:         rangeEnd,
		includeCols:      includedCols,
	}, nil
}

func (p *PullIndexReader) GetRows(limit int) (rows *common.Rows, err error) {
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
			// Just construct row from key
			// TODO
		} else {
			// Lookup row in table
			// TODO
		}
	}
	return rows, nil
}
