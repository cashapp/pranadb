package exec

import (
	"math"

	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/errors"
)

// PullLimit is an executor for LIMIT <offset>, <count> statement and its variations.
type PullLimit struct {
	pullExecutorBase
	count  uint64
	offset uint64
	rows   *common.Rows
	cursor int
}

func NewPullLimit(colNames []string, colTypes []common.ColumnType, count, offset uint64) *PullLimit {
	rf := common.NewRowsFactory(colTypes)
	base := pullExecutorBase{
		colNames:       colNames,
		colTypes:       colTypes,
		simpleColNames: common.ToSimpleColNames(colNames),
		rowsFactory:    rf,
	}
	return &PullLimit{
		pullExecutorBase: base,
		count:            count,
		offset:           offset,
	}
}

func (l *PullLimit) GetRows(limit int) (*common.Rows, error) {
	if limit < 1 {
		return nil, errors.Errorf("invalid limit %d", limit)
	}
	// Offset and count are parsed as uint64s, we must check if their sum fits an int.
	if l.offset > math.MaxInt32 {
		return nil, errors.Errorf("limit offset %d cannot be larger than %d", l.offset, math.MaxInt32)
	}
	offset := int(l.offset)
	if l.count > math.MaxInt32 {
		return nil, errors.Errorf("limit count %d cannot be larger than %d", l.count, math.MaxInt32)
	}
	offsetAndCount := l.offset + l.count
	if offsetAndCount > math.MaxInt32 {
		return nil, errors.Errorf("limit offset and count cannot be larger than %d", math.MaxInt32)
	}
	if l.count == 0 {
		return l.rowsFactory.NewRows(0), nil
	}
	if l.rows == nil {
		var err error
		l.rows, err = l.GetChildren()[0].GetRows(int(offsetAndCount))
		if err != nil {
			return nil, errors.WithStack(err)
		}
		for { // Drain all rows so that the current session query is cleared.
			rows, err := l.GetChildren()[0].GetRows(batchSize)
			if err != nil {
				return nil, errors.WithStack(err)
			}
			if rows.RowCount() < batchSize {
				break
			}
		}
	}
	startIndex := offset + l.cursor
	if startIndex >= l.rows.RowCount() {
		return l.rowsFactory.NewRows(0), nil
	}
	endIndex := startIndex + limit
	if endIndex > l.rows.RowCount() {
		endIndex = l.rows.RowCount()
	}
	l.cursor += limit
	if startIndex == 0 && endIndex >= l.rows.RowCount() {
		return l.rows, nil
	}
	result := l.rowsFactory.NewRows(endIndex - startIndex)
	for i := startIndex; i < endIndex; i++ {
		result.AppendRow(l.rows.GetRow(i))
	}
	return result, nil
}
