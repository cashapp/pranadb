package exec

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/common"
)

func TestProjectionOneCol(t *testing.T) {
	inpRows := [][]interface{}{
		{1, "wincanton", 25.5, "132.45"},
		{2, "london", 35.1, "9.32"},
		{3, "los angeles", 20.6, "11.75"},
	}
	expectedRows := [][]interface{}{
		{"wincanton"},
		{"london"},
		{"los angeles"},
	}
	expectedColTypes := []common.ColumnType{common.VarcharColumnType}
	expectedColNames := []string{"location"}
	testProject(t, inpRows, expectedRows, expectedColNames, expectedColTypes, colExpression(1))
}

func TestProjectionAllCols(t *testing.T) {
	inpRows := [][]interface{}{
		{1, "wincanton", 25.5, "132.45"},
		{2, "london", 35.1, "9.32"},
		{3, "los angeles", 20.6, "11.75"},
	}
	expectedRows := [][]interface{}{
		{1, "wincanton", 25.5, "132.45"},
		{2, "london", 35.1, "9.32"},
		{3, "los angeles", 20.6, "11.75"},
	}
	testProject(t, inpRows, expectedRows, colNames, colTypes, colExpression(0), colExpression(1), colExpression(2), colExpression(3))
}

func TestProjectionAllColsReverseOrder(t *testing.T) {
	inpRows := [][]interface{}{
		{1, "wincanton", 25.5, "132.45"},
		{2, "london", 35.1, "9.32"},
		{3, "los angeles", 20.6, "11.75"},
	}
	expectedRows := [][]interface{}{
		{"132.45", 25.5, "wincanton", 1},
		{"9.32", 35.1, "london", 2},
		{"11.75", 20.6, "los angeles", 3},
	}
	columnNames := []string{"temperature", "location", "sensor_id"}
	columnTypes := []common.ColumnType{common.NewDecimalColumnType(10, 2), common.DoubleColumnType, common.VarcharColumnType, common.BigIntColumnType}
	testProject(t, inpRows, expectedRows, columnNames, columnTypes, colExpression(3), colExpression(2), colExpression(1), colExpression(0))
}

func TestProjectionNonColExpression(t *testing.T) {
	inpRows := [][]interface{}{
		{1, "wincanton", 25.5, "132.45"},
		{2, "london", 35.1, "9.32"},
		{3, "los angeles", 20.6, "11.75"},
	}
	expectedRows := [][]interface{}{
		{1, "wincanton", 26.5, "132.45"},
		{2, "london", 36.1, "9.32"},
		{3, "los angeles", 21.6, "11.75"},
	}
	con := common.NewConstantInt(common.BigIntColumnType, 1)
	// Add one to column 2
	f, err := common.NewScalarFunctionExpression(colTypes[2], "plus", colExpression(2), con)
	require.NoError(t, err)
	testProject(t, inpRows, expectedRows, colNames, colTypes, colExpression(0), colExpression(1), f, colExpression(3))
}

func testProject(t *testing.T, inputRows [][]interface{}, expectedRows [][]interface{}, expectedColNames []string, expectedColTypes []common.ColumnType, projExprs ...*common.Expression) {
	t.Helper()
	proj := NewPushProjection(expectedColNames, expectedColTypes, projExprs)
	execCtx := &ExecutionContext{
		WriteBatch: cluster.NewWriteBatch(1, false),
	}
	rg := &rowGatherer{}
	proj.SetParent(rg)

	inpRows := toRows(t, inputRows, colTypes)
	err := proj.HandleRows(inpRows, execCtx)
	require.NoError(t, err)

	gathered := rg.Rows
	require.NotNil(t, gathered)

	exp := toRows(t, expectedRows, expectedColTypes)
	common.AllRowsEqual(t, exp, gathered, expectedColTypes)
}
