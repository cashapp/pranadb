package exec

import (
	"github.com/squareup/pranadb/common"
	"github.com/stretchr/testify/require"
	"testing"
)

// Test utils for this package

var colNames = []string{"sensor_id", "location", "temperature", "cost"}
var colTypes = []common.ColumnType{common.BigIntColumnType, common.VarcharColumnType, common.DoubleColumnType, common.NewDecimalColumnType(10, 2)}

func toRows(t *testing.T, rows [][]interface{}, colTypes []common.ColumnType) *common.Rows {
	rf, err := common.NewRowsFactory(colTypes)
	require.Nil(t, err)
	r := rf.NewRows(len(rows))
	for _, row := range rows {
		common.AppendRow(t, r, colTypes, row...)
	}
	return r
}

func colExpression(t *testing.T, colIndex int) *common.Expression {
	col, err := common.NewColumnExpression(colIndex, colTypes[colIndex])
	require.Nil(t, err)
	return col
}

func constDoubleExpression(t *testing.T, colIndex int, val float64) *common.Expression {
	con, err := common.NewConstantDouble(colTypes[colIndex], val)
	require.Nil(t, err)
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
