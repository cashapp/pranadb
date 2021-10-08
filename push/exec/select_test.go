package exec

import (
	"testing"

	"github.com/squareup/pranadb/common/commontest"

	"github.com/stretchr/testify/require"

	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/common"
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
	testSelect(t, inpRows, expectedRows, "gt", colExpression(2), constDoubleExpression(2, 28.0))
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
	testSelect(t, inpRows, expectedRows, "gt", colExpression(2), constDoubleExpression(2, 15.0))
}

func TestSelectNoRows(t *testing.T) {
	inpRows := [][]interface{}{
		{1, "wincanton", 25.5, "132.45"},
		{2, "london", 35.1, "9.32"},
		{3, "los angeles", 20.6, "11.75"},
	}
	var expectedRows [][]interface{}
	testSelect(t, inpRows, expectedRows, "gt", colExpression(2), constDoubleExpression(2, 40.0))
}

func testSelect(t *testing.T, inputRows [][]interface{}, expectedRows [][]interface{}, funcName string, funcArgs ...*common.Expression) {
	t.Helper()
	predicate, err := common.NewScalarFunctionExpression(colTypes[2], funcName, funcArgs...)
	require.NoError(t, err)
	sel := NewPushSelect([]*common.Expression{predicate})
	sel.calculateSchema(colTypes, nil)
	require.NoError(t, err)
	execCtx := &ExecutionContext{
		WriteBatch: cluster.NewWriteBatch(1, false),
	}
	rg := &rowGatherer{}
	sel.SetParent(rg)

	inpRows := toRows(t, inputRows, colTypes)
	err = sel.HandleRows(NewCurrentRowsBatch(inpRows), execCtx)
	require.NoError(t, err)

	gathered := rg.Rows
	require.NotNil(t, gathered)

	exp := toRows(t, expectedRows, colTypes)
	commontest.AllRowsEqual(t, exp, gathered, colTypes)
}
