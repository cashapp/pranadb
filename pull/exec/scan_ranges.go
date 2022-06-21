package exec

import (
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/table"
)

func calcScanRangeKeys(scanRanges []*ScanRange, indexID uint64, indexCols []int, tableInfo *common.TableInfo,
	shardID uint64, isIndex bool) ([]*rangeHolder, error) {
	keyPrefix := table.EncodeTableKeyPrefix(indexID, shardID, 16)
	if len(scanRanges) == 1 && scanRanges[0] == nil {
		return []*rangeHolder{{rangeStart: keyPrefix, rangeEnd: table.EncodeTableKeyPrefix(indexID+1, shardID, 16)}}, nil
	}
	rangeHolders := make([]*rangeHolder, len(scanRanges))
	for i, sr := range scanRanges {
		rangeStart := append([]byte{}, keyPrefix...)
		rangeEnd := append([]byte{}, keyPrefix...)
		var err error
		for j := 0; j < len(sr.LowVals); j++ {
			lv := sr.LowVals[j]
			hv := sr.HighVals[j]
			if lv == nil && hv == nil {
				// This represents a get of a null value from the index, it can't occur for a pk
				if !isIndex {
					panic("get of null in pk index")
				}
				rangeStart = append(rangeStart, 0)
				rangeEnd = append(rangeEnd, 0)
			} else if hv != nil {
				// This is a closed range
				if isIndex {
					// Only index keys have a marker byte which says whether the key element is null or not
					rangeEnd = append(rangeEnd, 1)
				}
				rangeEnd, err = common.EncodeKeyElement(hv, tableInfo.ColumnTypes[indexCols[j]], rangeEnd)
				if err != nil {
					return nil, err
				}
			}
			if lv != nil {
				if isIndex {
					rangeStart = append(rangeStart, 1)
				}
				rangeStart, err = common.EncodeKeyElement(lv, tableInfo.ColumnTypes[indexCols[j]], rangeStart)
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
		if rangeEnd == nil {
			rangeEnd = table.EncodeTableKeyPrefix(indexID+1, shardID, 16)
		}
		rangeHolders[i] = &rangeHolder{
			rangeStart: rangeStart,
			rangeEnd:   rangeEnd,
		}
	}
	return rangeHolders, nil
}
