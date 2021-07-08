package exec

import (
	"testing"

	"github.com/stretchr/testify/require"

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
		err := common.AppendRow(t, r, colTypes, row...)
		require.NoError(t, err)
	}
	return r
}

func colExpression(colIndex int) *common.Expression {
	col := common.NewColumnExpression(colIndex, colTypes[colIndex])
	return col
}

func constDoubleExpression(colIndex int, val float64) *common.Expression {
	con := common.NewConstantDouble(colTypes[colIndex], val)
	return con
}

type rowGatherer struct {
	Rows *common.Rows
}

func (r *rowGatherer) HandleRows(rows *common.Rows, ctx *ExecutionContext) error {
	r.Rows = rows
	return nil
}

func (r rowGatherer) SetParent(parent PushExecutor) {
}

func (r rowGatherer) GetParent() PushExecutor {
	return nil
}

func (r rowGatherer) AddChild(parent PushExecutor) {
}

func (r rowGatherer) GetChildren() []PushExecutor {
	return nil
}

func (r rowGatherer) ClearChildren() {
}

func (r rowGatherer) ReCalcSchemaFromChildren() {
}

func (r rowGatherer) ColNames() []string {
	return nil
}

func (r rowGatherer) ColTypes() []common.ColumnType {
	return nil
}

func (r rowGatherer) KeyCols() []int {
	return nil
}
