package exec

import (
	"strconv"

	"github.com/squareup/pranadb/common"
)

// Empty executor.
var Empty = &StaticRows{rows: common.NewRows(nil, 0)}

type StaticRows struct {
	pullExecutorBase
	rows *common.Rows
}

var _ PullExecutor = &StaticRows{}

// NewStaticRow creates a static single row result.
func NewStaticRow(values ...interface{}) *StaticRows {
	row := common.InferRow(values...)
	rows := common.NewRows(row.ColumnTypes(), 1)
	colNames := make([]string, len(values))
	for i := 0; i < len(values); i++ {
		colNames[i] = strconv.Itoa(i)
	}
	rows.AppendRow(row)
	return NewStaticRows(colNames, rows)
}

// NewStaticRows creates a static set of rows.
func NewStaticRows(colNames []string, rows *common.Rows) *StaticRows {
	return &StaticRows{
		pullExecutorBase: pullExecutorBase{
			colTypes: rows.ColumnTypes(),
			colNames: colNames,
		},
		rows: rows,
	}
}

func (s *StaticRows) GetRows(limit int) (rows *common.Rows, err error) {
	return s.rows, nil
}
