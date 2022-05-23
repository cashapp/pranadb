package exec

import (
	"fmt"

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

func (l *PullLimit) GetRows(maxRowsToReturn int) (*common.Rows, error) {
	if maxRowsToReturn < 1 {
		return nil, errors.Errorf("invalid limit %d", maxRowsToReturn)
	}
	// OFFSET is unsupported for now.
	if l.offset != 0 {
		return nil, errors.NewInvalidStatementError("offset must be zero")
	}
	// Because LIMIT is often used together with ORDER BY which is limited to orderByMaxRows rows,
	// we impose the same max on LIMIT.
	if l.count > orderByMaxRows {
		return nil, errors.NewInvalidStatementError(
			fmt.Sprintf("limit count cannot be larger than %d", orderByMaxRows),
		)
	}
	if l.count == 0 {
		return l.rowsFactory.NewRows(0), nil
	}
	if l.rows == nil {
		child := l.GetChildren()[0]
		var err error
		l.rows, err = child.GetRows(int(l.count))
		if err != nil {
			return nil, errors.WithStack(err)
		}
		for {
			// Drain all rows so that the current session query is cleared.
			// TODO: Add a method to stop PullExecutor early
			// https://github.com/cashapp/pranadb/issues/361
			rows, err := child.GetRows(batchSize)
			if err != nil {
				return nil, errors.WithStack(err)
			}
			if rows.RowCount() < batchSize {
				break
			}
		}
	}
	startIndex := l.cursor
	endIndex := startIndex + maxRowsToReturn
	if endIndex > l.rows.RowCount() {
		endIndex = l.rows.RowCount()
	}
	l.cursor = endIndex
	if startIndex == 0 && endIndex >= l.rows.RowCount() {
		return l.rows, nil
	}
	capacity := endIndex - startIndex
	if capacity < 0 {
		capacity = 0
	}
	result := l.rowsFactory.NewRows(capacity)
	for i := startIndex; i < endIndex; i++ {
		result.AppendRow(l.rows.GetRow(i))
	}
	return result, nil
}
