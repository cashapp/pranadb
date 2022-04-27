package commontest

import (
	"testing"

	"github.com/squareup/pranadb/common"

	"github.com/stretchr/testify/require"
)

func TestColumnExpressionTinyInt(t *testing.T) {
	row := createRow(t)
	colExpr := common.NewColumnExpression(0, common.TinyIntColumnType)
	val, null, err := colExpr.EvalInt64(row)
	require.NoError(t, err)
	require.False(t, null)
	require.Equal(t, int64(1), val)
}

func TestColumnExpressionNullTinyInt(t *testing.T) {
	row := createRow(t)
	colExpr := common.NewColumnExpression(1, common.TinyIntColumnType)
	_, null, err := colExpr.EvalInt64(row)
	require.NoError(t, err)
	require.True(t, null)
}

func TestColumnExpressionInt(t *testing.T) {
	row := createRow(t)
	colExpr := common.NewColumnExpression(2, common.IntColumnType)
	val, null, err := colExpr.EvalInt64(row)
	require.NoError(t, err)
	require.False(t, null)
	require.Equal(t, int64(2), val)
}

func TestColumnExpressionNullInt(t *testing.T) {
	row := createRow(t)
	colExpr := common.NewColumnExpression(3, common.IntColumnType)
	_, null, err := colExpr.EvalInt64(row)
	require.NoError(t, err)
	require.True(t, null)
}

func TestColumnExpressionBigInt(t *testing.T) {
	row := createRow(t)
	colExpr := common.NewColumnExpression(4, common.BigIntColumnType)
	val, null, err := colExpr.EvalInt64(row)
	require.NoError(t, err)
	require.False(t, null)
	require.Equal(t, int64(3), val)
}

func TestColumnExpressionNullBigInt(t *testing.T) {
	row := createRow(t)
	colExpr := common.NewColumnExpression(5, common.BigIntColumnType)
	_, null, err := colExpr.EvalInt64(row)
	require.NoError(t, err)
	require.True(t, null)
}

func TestColumnExpressionDouble(t *testing.T) {
	row := createRow(t)
	colExpr := common.NewColumnExpression(6, common.DoubleColumnType)
	val, null, err := colExpr.EvalFloat64(row)
	require.NoError(t, err)
	require.False(t, null)
	require.Equal(t, 1.23, val)
}

func TestColumnExpressionNullDouble(t *testing.T) {
	row := createRow(t)
	colExpr := common.NewColumnExpression(7, common.DoubleColumnType)
	_, null, err := colExpr.EvalFloat64(row)
	require.NoError(t, err)
	require.True(t, null)
}

func TestColumnExpressionVarchar(t *testing.T) {
	row := createRow(t)
	colExpr := common.NewColumnExpression(8, common.VarcharColumnType)
	val, null, err := colExpr.EvalString(row)
	require.NoError(t, err)
	require.False(t, null)
	require.Equal(t, "some-string", val)
}

func TestColumnExpressionNullVarchar(t *testing.T) {
	row := createRow(t)
	colExpr := common.NewColumnExpression(9, common.VarcharColumnType)
	_, null, err := colExpr.EvalString(row)
	require.NoError(t, err)
	require.True(t, null)
}

func TestColumnExpressionDecimal(t *testing.T) {
	row := createRow(t)
	colExpr := common.NewColumnExpression(10, common.NewDecimalColumnType(10, 2))
	val, null, err := colExpr.EvalDecimal(row)
	require.NoError(t, err)
	require.False(t, null)
	dec, err := common.NewDecFromString("12345.54321")
	require.NoError(t, err)
	require.Equal(t, dec.String(), val.String())
}

func TestColumnExpressionNullDecimal(t *testing.T) {
	row := createRow(t)
	colExpr := common.NewColumnExpression(11, common.NewDecimalColumnType(10, 2))
	_, null, err := colExpr.EvalDecimal(row)
	require.NoError(t, err)
	require.True(t, null)
}

func createRow(t *testing.T) *common.Row {
	t.Helper()
	decType1 := common.NewDecimalColumnType(10, 2)
	colTypes := []common.ColumnType{common.TinyIntColumnType, common.TinyIntColumnType, common.IntColumnType, common.IntColumnType, common.BigIntColumnType, common.BigIntColumnType, common.DoubleColumnType, common.DoubleColumnType, common.VarcharColumnType, common.VarcharColumnType, decType1, decType1}
	rf := common.NewRowsFactory(colTypes)
	rows := rf.NewRows(1)
	rows.AppendInt64ToColumn(0, 1)
	rows.AppendNullToColumn(1)
	rows.AppendInt64ToColumn(2, 2)
	rows.AppendNullToColumn(3)
	rows.AppendInt64ToColumn(4, 3)
	rows.AppendNullToColumn(5)
	rows.AppendFloat64ToColumn(6, 1.23)
	rows.AppendNullToColumn(7)
	rows.AppendStringToColumn(8, "some-string")
	rows.AppendNullToColumn(9)
	dec, err := common.NewDecFromString("12345.54321")
	require.NoError(t, err)
	rows.AppendDecimalToColumn(10, *dec)
	rows.AppendNullToColumn(11)
	row := rows.GetRow(0)
	return &row
}

func TestConstantTinyIntExpression(t *testing.T) {
	row := createRow(t)
	expr := common.NewConstantInt(common.TinyIntColumnType, 100)
	val, null, err := expr.EvalInt64(row)
	require.NoError(t, err)
	require.False(t, null)
	require.Equal(t, int64(100), val)
}

func TestConstantIntExpression(t *testing.T) {
	row := createRow(t)
	expr := common.NewConstantInt(common.IntColumnType, 101)
	val, null, err := expr.EvalInt64(row)
	require.NoError(t, err)
	require.False(t, null)
	require.Equal(t, int64(101), val)
}

func TestConstantBigIntExpression(t *testing.T) {
	row := createRow(t)
	expr := common.NewConstantInt(common.BigIntColumnType, 102)
	val, null, err := expr.EvalInt64(row)
	require.NoError(t, err)
	require.False(t, null)
	require.Equal(t, int64(102), val)
}

func TestConstantDoubleExpression(t *testing.T) {
	row := createRow(t)
	expr := common.NewConstantDouble(common.DoubleColumnType, 1234.32)
	val, null, err := expr.EvalFloat64(row)
	require.NoError(t, err)
	require.False(t, null)
	require.Equal(t, 1234.32, val)
}

func TestConstantStringExpression(t *testing.T) {
	row := createRow(t)
	expr := common.NewConstantVarchar(common.VarcharColumnType, "other-string")
	val, null, err := expr.EvalString(row)
	require.NoError(t, err)
	require.False(t, null)
	require.Equal(t, "other-string", val)
}

func TestScalarFunctionExpression(t *testing.T) {
	row := createRow(t)
	colExpr1 := common.NewColumnExpression(0, common.TinyIntColumnType)
	colExpr2 := common.NewColumnExpression(2, common.IntColumnType)
	expr1, err := common.NewScalarFunctionExpression(common.BigIntColumnType, "gt", colExpr2, colExpr1)
	require.NoError(t, err)
	val, null, err := expr1.EvalInt64(row)
	require.NoError(t, err)
	require.False(t, null)
	require.Equal(t, int64(1), val)
}
