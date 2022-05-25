package exec

import (
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/errors"
)

// Empty executor.
var Empty = &StaticRows{rows: common.NewRows(nil, 0)}

type StaticRows struct {
	pullExecutorBase
	rows   *common.Rows
	cursor int
}

var _ PullExecutor = &StaticRows{}

func NewSingleValueBigIntRow(val int64, colName string) *StaticRows {
	colTypes := []common.ColumnType{common.BigIntColumnType}
	rf := common.NewRowsFactory(colTypes)
	rows := rf.NewRows(1)
	rows.AppendInt64ToColumn(0, val)
	colNames := []string{colName}
	return &StaticRows{
		pullExecutorBase: pullExecutorBase{
			colTypes:       rows.ColumnTypes(),
			colNames:       colNames,
			simpleColNames: colNames,
		},
		rows: rows,
	}
}

// NewStaticRows created static rows pull executor.
func NewStaticRows(colNames []string, rows *common.Rows) (*StaticRows, error) {
	sr := &StaticRows{
		pullExecutorBase: pullExecutorBase{},
		rows:             rows,
	}
	if rows == nil || rows.RowCount() == 0 {
		return sr, nil
	}
	row := rows.GetRow(0)
	colTypes := row.ColumnTypes()
	if len(colNames) != len(colTypes) {
		return nil, errors.Errorf("got different length of column names and column types arrays: colNames=%d, colTypes=%d", len(colNames), len(colTypes))
	}
	sr.colNames = colNames
	sr.simpleColNames = colNames
	sr.colTypes = colTypes
	return sr, nil
}

func (s *StaticRows) GetRows(limit int) (rows *common.Rows, err error) {
	pageEnd := s.cursor + limit
	if pageEnd > s.rows.RowCount() {
		pageEnd = s.rows.RowCount()
	}
	buffer := common.NewRows(s.rows.ColumnTypes(), pageEnd-s.cursor)
	for ; s.cursor < pageEnd; s.cursor++ {
		buffer.AppendRow(s.rows.GetRow(s.cursor))
	}
	return buffer, nil
}
