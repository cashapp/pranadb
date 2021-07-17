package exec

import (
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/common/commontest"
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
	exp := toRows(t, expectedRows, expectedColTypes)
	proj := setupProject(t, inpRows, expectedColNames, expectedColTypes, colExpression(1))

	provided, err := proj.GetRows(-1)
	require.NoError(t, err)
	require.NotNil(t, provided)
	commontest.AllRowsEqual(t, exp, provided, expectedColTypes)

	provided, err = proj.GetRows(-1)
	require.NoError(t, err)
	require.NotNil(t, provided)
	require.Equal(t, 0, provided.RowCount())
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
	exp := toRows(t, expectedRows, colTypes)
	proj := setupProject(t, inpRows, colNames, colTypes, colExpression(0), colExpression(1), colExpression(2), colExpression(3))

	provided, err := proj.GetRows(-1)
	require.NoError(t, err)
	require.NotNil(t, provided)
	commontest.AllRowsEqual(t, exp, provided, colTypes)

	provided, err = proj.GetRows(-1)
	require.NoError(t, err)
	require.NotNil(t, provided)
	require.Equal(t, 0, provided.RowCount())
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
	expectedColNames := []string{"temperature", "location", "sensor_id"}
	expectedColTypes := []common.ColumnType{common.NewDecimalColumnType(10, 2), common.DoubleColumnType, common.VarcharColumnType, common.BigIntColumnType}
	exp := toRows(t, expectedRows, expectedColTypes)
	proj := setupProject(t, inpRows, expectedColNames, expectedColTypes, colExpression(3), colExpression(2), colExpression(1), colExpression(0))

	provided, err := proj.GetRows(-1)
	require.NoError(t, err)
	require.NotNil(t, provided)
	require.Equal(t, exp.RowCount(), provided.RowCount())
	commontest.AllRowsEqual(t, exp, provided, expectedColTypes)

	provided, err = proj.GetRows(-1)
	require.NoError(t, err)
	require.NotNil(t, provided)
	require.Equal(t, 0, provided.RowCount())
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
	exp := toRows(t, expectedRows, colTypes)
	con := common.NewConstantInt(common.BigIntColumnType, 1)
	// Add one to column 2
	f, err := common.NewScalarFunctionExpression(colTypes[2], "plus", colExpression(2), con)
	require.NoError(t, err)

	proj := setupProject(t, inpRows, colNames, colTypes, colExpression(0), colExpression(1), f, colExpression(3))

	provided, err := proj.GetRows(-1)
	require.NoError(t, err)
	require.NotNil(t, provided)
	require.Equal(t, exp.RowCount(), provided.RowCount())
	commontest.AllRowsEqual(t, exp, provided, colTypes)

	provided, err = proj.GetRows(-1)
	require.NoError(t, err)
	require.NotNil(t, provided)
	require.Equal(t, 0, provided.RowCount())

}

func TestProjectionWithLimit2(t *testing.T) {
	inpRows := [][]interface{}{
		{1, "wincanton", 25.5, "132.45"},
		{2, "london", 35.1, "9.32"},
		{3, "los angeles", 20.6, "11.75"},
		{4, "sydney", 45.2, "4.99"},
		{5, "tokyo", 28.9, "999.99"},
	}

	proj := setupProject(t, inpRows, colNames, colTypes, colExpression(0), colExpression(1), colExpression(2), colExpression(3))

	expectedRows1 := [][]interface{}{
		{1, "wincanton", 25.5, "132.45"},
		{2, "london", 35.1, "9.32"},
	}
	exp1 := toRows(t, expectedRows1, colTypes)
	provided, err := proj.GetRows(2)
	require.NoError(t, err)
	require.NotNil(t, provided)
	commontest.AllRowsEqual(t, exp1, provided, colTypes)

	expectedRows2 := [][]interface{}{
		{3, "los angeles", 20.6, "11.75"},
		{4, "sydney", 45.2, "4.99"},
	}
	exp2 := toRows(t, expectedRows2, colTypes)
	provided, err = proj.GetRows(2)
	require.NoError(t, err)
	require.NotNil(t, provided)
	commontest.AllRowsEqual(t, exp2, provided, colTypes)

	expectedRows3 := [][]interface{}{
		{5, "tokyo", 28.9, "999.99"},
	}
	exp3 := toRows(t, expectedRows3, colTypes)
	provided, err = proj.GetRows(2)
	require.NoError(t, err)
	require.NotNil(t, provided)
	commontest.AllRowsEqual(t, exp3, provided, colTypes)

	provided, err = proj.GetRows(2)
	require.NoError(t, err)
	require.NotNil(t, provided)
	require.Equal(t, 0, provided.RowCount())
}

func TestProjectionWithLimit0(t *testing.T) {
	inpRows := [][]interface{}{
		{1, "wincanton", 25.5, "132.45"},
		{2, "london", 35.1, "9.32"},
		{3, "los angeles", 20.6, "11.75"},
		{4, "sydney", 45.2, "4.99"},
		{5, "tokyo", 28.9, "999.99"},
	}

	proj := setupProject(t, inpRows, colNames, colTypes, colExpression(0), colExpression(1), colExpression(2), colExpression(3))

	_, err := proj.GetRows(0)
	require.NotNil(t, err)
}

func setupProject(t *testing.T, inputRows [][]interface{}, expectedColNames []string, expectedColTypes []common.ColumnType, projExprs ...*common.Expression) PullExecutor {
	t.Helper()
	proj, err := NewPullProjection(expectedColNames, expectedColTypes, projExprs)
	require.NoError(t, err)

	inpRows := toRows(t, inputRows, colTypes)

	rf := common.NewRowsFactory(colTypes)
	rowsProvider := rowProvider{
		rowsFactory: rf,
		rows:        inpRows,
	}
	proj.AddChild(&rowsProvider)

	return proj
}
