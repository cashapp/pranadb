package exec

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/storage"
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
	require.Nil(t, err)
	sel := NewPushSelect(colNames, colTypes, []*common.Expression{predicate})
	require.Nil(t, err)
	execCtx := &ExecutionContext{
		WriteBatch: storage.NewWriteBatch(1),
	}
	rg := &rowGatherer{}
	sel.SetParent(rg)

	inpRows := toRows(t, inputRows, colTypes)
	err = sel.HandleRows(inpRows, execCtx)
	require.Nil(t, err)

	gathered := rg.Rows
	require.NotNil(t, gathered)

	exp := toRows(t, expectedRows, colTypes)
	require.Equal(t, exp.RowCount(), gathered.RowCount())
	for i := 0; i < exp.RowCount(); i++ {
		common.RowsEqual(t, exp.GetRow(i), gathered.GetRow(i), colTypes)
	}
}
