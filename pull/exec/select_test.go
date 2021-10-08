package exec

import (
	"testing"

	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/common/commontest"
	"github.com/stretchr/testify/require"
)

func TestSelectOneRow(t *testing.T) {
	inpRows := [][]interface{}{
		{1, "wincanton", 25.5, "132.45"},
		{2, "london", 35.1, "9.32"},
		{3, "los angeles", 20.6, "11.75"},
	}
	expectedRows := [][]interface{}{
		{2, "london", 35.1, "9.32"},
	}
	exp := toRows(t, expectedRows, colTypes)
	sel := setupSelect(t, inpRows, colExpression(2), constDoubleExpression(2, 28.0))

	provided, err := sel.GetRows(1000)
	require.NoError(t, err)
	require.NotNil(t, provided)

	commontest.AllRowsEqual(t, exp, provided, colTypes)

	provided, err = sel.GetRows(1000)
	require.NoError(t, err)
	require.NotNil(t, provided)
	require.Equal(t, 0, provided.RowCount())
}

func TestSelectAllRows(t *testing.T) {
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
	sel := setupSelect(t, inpRows, colExpression(2), constDoubleExpression(2, 15.0))

	provided, err := sel.GetRows(1000)
	require.NoError(t, err)
	require.NotNil(t, provided)
	expectedCount := len(expectedRows)
	require.Equal(t, expectedCount, provided.RowCount())
	commontest.AllRowsEqual(t, exp, provided, colTypes)

	provided, err = sel.GetRows(1000)
	require.NoError(t, err)
	require.NotNil(t, provided)
	require.Equal(t, 0, provided.RowCount())
}

func TestSelectNoRows(t *testing.T) {
	inpRows := [][]interface{}{
		{1, "wincanton", 25.5, "132.45"},
		{2, "london", 35.1, "9.32"},
		{3, "los angeles", 20.6, "11.75"},
	}
	sel := setupSelect(t, inpRows, colExpression(2), constDoubleExpression(2, 40.0))

	provided, err := sel.GetRows(1000)
	require.NoError(t, err)
	require.NotNil(t, provided)
	require.Equal(t, 0, provided.RowCount())

	// And once more for good measure
	provided, err = sel.GetRows(1000)
	require.NoError(t, err)
	require.NotNil(t, provided)
	require.Equal(t, 0, provided.RowCount())
}

func TestSelectWithLimit0(t *testing.T) {
	inpRows := [][]interface{}{
		{1, "wincanton", 25.5, "132.45"},
		{2, "london", 35.1, "9.32"},
		{3, "los angeles", 20.6, "11.75"},
	}
	sel := setupSelect(t, inpRows, colExpression(2), constDoubleExpression(2, 0))

	_, err := sel.GetRows(0)
	require.NotNil(t, err)
}

func TestSelectWithLimit2(t *testing.T) {
	inpRows := [][]interface{}{
		{1, "wincanton", 25.5, "132.45"},
		{2, "london", 35.1, "9.32"},
		{3, "los angeles", 20.6, "11.75"},
		{4, "sydney", 45.2, "4.99"},
		{5, "tokyo", 28.9, "999.99"},
	}

	sel := setupSelect(t, inpRows, colExpression(2), constDoubleExpression(2, 0))

	expectedRows1 := [][]interface{}{
		{1, "wincanton", 25.5, "132.45"},
		{2, "london", 35.1, "9.32"},
	}
	exp1 := toRows(t, expectedRows1, colTypes)
	provided, err := sel.GetRows(2)
	require.NoError(t, err)
	require.NotNil(t, provided)
	commontest.AllRowsEqual(t, exp1, provided, colTypes)

	expectedRows2 := [][]interface{}{
		{3, "los angeles", 20.6, "11.75"},
		{4, "sydney", 45.2, "4.99"},
	}
	exp2 := toRows(t, expectedRows2, colTypes)
	provided, err = sel.GetRows(2)
	require.NoError(t, err)
	require.NotNil(t, provided)
	commontest.AllRowsEqual(t, exp2, provided, colTypes)

	expectedRows3 := [][]interface{}{
		{5, "tokyo", 28.9, "999.99"},
	}
	exp3 := toRows(t, expectedRows3, colTypes)
	provided, err = sel.GetRows(2)
	require.NoError(t, err)
	require.NotNil(t, provided)
	commontest.AllRowsEqual(t, exp3, provided, colTypes)

	provided, err = sel.GetRows(2)
	require.NoError(t, err)
	require.NotNil(t, provided)
	require.Equal(t, 0, provided.RowCount())
}

func setupSelect(t *testing.T, inputRows [][]interface{}, funcArgs ...*common.Expression) PullExecutor {
	t.Helper()
	predicate, err := common.NewScalarFunctionExpression(colTypes[2], "gt", funcArgs...)
	require.NoError(t, err)
	sel := NewPullSelect(colNames, colTypes, []*common.Expression{predicate})

	inpRows := toRows(t, inputRows, colTypes)
	rf := common.NewRowsFactory(colTypes)
	rowsProvider := rowProvider{
		rowsFactory: rf,
		rows:        inpRows,
	}
	sel.AddChild(&rowsProvider)

	return sel
}
