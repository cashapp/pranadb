package exec

import (
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/storage"
	"github.com/stretchr/testify/require"
	"testing"
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
	testProject(t, inpRows, expectedRows, expectedColNames, expectedColTypes, colExpression(t, 1))
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
	testProject(t, inpRows, expectedRows, colNames, colTypes, colExpression(t, 0), colExpression(t, 1), colExpression(t, 2), colExpression(t, 3))
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
	colNames := []string{"temperature", "location", "sensor_id"}
	colTypes := []common.ColumnType{common.NewDecimalColumnType(10, 2), common.DoubleColumnType, common.VarcharColumnType, common.BigIntColumnType}
	testProject(t, inpRows, expectedRows, colNames, colTypes, colExpression(t, 3), colExpression(t, 2), colExpression(t, 1), colExpression(t, 0))
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
	con, err := common.NewConstantInt(common.BigIntColumnType, 1)
	require.Nil(t, err)
	// Add one to column 2
	f, err := common.NewScalarFunctionExpression(colTypes[2], "plus", colExpression(t, 2), con)
	require.Nil(t, err)
	testProject(t, inpRows, expectedRows, colNames, colTypes, colExpression(t, 0), colExpression(t, 1), f, colExpression(t, 3))
}

func testProject(t *testing.T, inputRows [][]interface{}, expectedRows [][]interface{}, expectedColNames []string, expectedColTypes []common.ColumnType, projExprs ...*common.Expression) {
	proj, err := NewPushProjection(expectedColNames, expectedColTypes, projExprs)
	require.Nil(t, err)
	execCtx := &ExecutionContext{
		WriteBatch: storage.NewWriteBatch(1),
	}
	rg := &rowGatherer{}
	proj.SetParent(rg)

	inpRows := toRows(t, inputRows, colTypes)
	err = proj.HandleRows(inpRows, execCtx)
	require.Nil(t, err)

	gathered := rg.Rows
	require.NotNil(t, gathered)

	exp := toRows(t, expectedRows, expectedColTypes)
	require.Equal(t, exp.RowCount(), gathered.RowCount())
	for i := 0; i < exp.RowCount(); i++ {
		common.RowsEqual(t, exp.GetRow(i), gathered.GetRow(i), expectedColTypes)
	}
}
