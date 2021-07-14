package exec

import (
	"testing"

	"github.com/squareup/pranadb/common"
)

// Test utils for this package

var colNames = []string{"sensor_id", "location", "temperature", "cost"}
var colTypes = []common.ColumnType{common.BigIntColumnType, common.VarcharColumnType, common.DoubleColumnType, common.NewDecimalColumnType(10, 2)}

func toRows(t *testing.T, rows [][]interface{}, colTypes []common.ColumnType) *common.Rows {
	t.Helper()
	rf := common.NewRowsFactory(colTypes)
	r := rf.NewRows(len(rows))
	for _, row := range rows {
		common.AppendRow(t, r, colTypes, row...)
	}
	return r
}

func colExpression(colIndex int) *common.Expression {
	col := common.NewColumnExpression(colIndex, colTypes[colIndex])
	return col
}

//nolint: unparam
func constDoubleExpression(colIndex int, val float64) *common.Expression {
	con := common.NewConstantDouble(colTypes[colIndex], val)
	return con
}

type rowProvider struct {
	rowsFactory *common.RowsFactory
	rows        *common.Rows
}

func (r *rowProvider) GetRows(limit int) (rows *common.Rows, err error) {
	rows = r.rowsFactory.NewRows(1)
	rowsRemaining := r.rowsFactory.NewRows(1)
	for i := 0; i < r.rows.RowCount(); i++ {
		if limit == -1 || i < limit {
			rows.AppendRow(r.rows.GetRow(i))
		} else {
			rowsRemaining.AppendRow(r.rows.GetRow(i))
		}
	}
	r.rows = rowsRemaining
	return
}

func (r *rowProvider) SetParent(parent PullExecutor) {
}

func (r *rowProvider) AddChild(child PullExecutor) {
}

func (r *rowProvider) GetParent() PullExecutor {
	return nil
}

func (r *rowProvider) GetChildren() []PullExecutor {
	return nil
}
