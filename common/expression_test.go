package common

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestColumnExpressionTinyInt(t *testing.T) {
	row := createRow(t)
	colExpr, err := NewColumnExpression(0, TinyIntColumnType)
	require.Nil(t, err)
	val, null, err := colExpr.EvalInt64(row)
	require.Nil(t, err)
	require.False(t, null)
	require.Equal(t, int64(1), val)
}

func TestColumnExpressionNullTinyInt(t *testing.T) {
	row := createRow(t)
	colExpr, err := NewColumnExpression(1, TinyIntColumnType)
	require.Nil(t, err)
	_, null, err := colExpr.EvalInt64(row)
	require.Nil(t, err)
	require.True(t, null)
}

func TestColumnExpressionInt(t *testing.T) {
	row := createRow(t)
	colExpr, err := NewColumnExpression(2, IntColumnType)
	require.Nil(t, err)
	val, null, err := colExpr.EvalInt64(row)
	require.Nil(t, err)
	require.False(t, null)
	require.Equal(t, int64(2), val)
}

func TestColumnExpressionNullInt(t *testing.T) {
	row := createRow(t)
	colExpr, err := NewColumnExpression(3, IntColumnType)
	require.Nil(t, err)
	_, null, err := colExpr.EvalInt64(row)
	require.Nil(t, err)
	require.True(t, null)
}

func TestColumnExpressionBigInt(t *testing.T) {
	row := createRow(t)
	colExpr, err := NewColumnExpression(4, BigIntColumnType)
	require.Nil(t, err)
	val, null, err := colExpr.EvalInt64(row)
	require.Nil(t, err)
	require.False(t, null)
	require.Equal(t, int64(3), val)
}

func TestColumnExpressionNullBigInt(t *testing.T) {
	row := createRow(t)
	colExpr, err := NewColumnExpression(5, BigIntColumnType)
	require.Nil(t, err)
	_, null, err := colExpr.EvalInt64(row)
	require.Nil(t, err)
	require.True(t, null)
}

func TestColumnExpressionDouble(t *testing.T) {
	row := createRow(t)
	colExpr, err := NewColumnExpression(6, DoubleColumnType)
	require.Nil(t, err)
	val, null, err := colExpr.EvalFloat64(row)
	require.Nil(t, err)
	require.False(t, null)
	require.Equal(t, 1.23, val)
}

func TestColumnExpressionNullDouble(t *testing.T) {
	row := createRow(t)
	colExpr, err := NewColumnExpression(7, DoubleColumnType)
	require.Nil(t, err)
	_, null, err := colExpr.EvalFloat64(row)
	require.Nil(t, err)
	require.True(t, null)
}

func TestColumnExpressionVarchar(t *testing.T) {
	row := createRow(t)
	colExpr, err := NewColumnExpression(8, VarcharColumnType)
	require.Nil(t, err)
	val, null, err := colExpr.EvalString(row)
	require.Nil(t, err)
	require.False(t, null)
	require.Equal(t, "some-string", val)
}

func TestColumnExpressionNullVarchar(t *testing.T) {
	row := createRow(t)
	colExpr, err := NewColumnExpression(9, VarcharColumnType)
	require.Nil(t, err)
	_, null, err := colExpr.EvalString(row)
	require.Nil(t, err)
	require.True(t, null)
}

func TestColumnExpressionDecimal(t *testing.T) {
	row := createRow(t)
	colExpr, err := NewColumnExpression(10, NewDecimalColumnType(10, 2))
	require.Nil(t, err)
	val, null, err := colExpr.EvalDecimal(row)
	require.Nil(t, err)
	require.False(t, null)
	dec, err := NewDecFromString("12345.54321")
	require.Nil(t, err)
	require.Equal(t, dec.ToString(), val.ToString())
}

func TestColumnExpressionNullDecimal(t *testing.T) {
	row := createRow(t)
	colExpr, err := NewColumnExpression(11, NewDecimalColumnType(10, 2))
	require.Nil(t, err)
	_, null, err := colExpr.EvalDecimal(row)
	require.Nil(t, err)
	require.True(t, null)
}

func createRow(t *testing.T) *Row {
	decType1 := NewDecimalColumnType(10, 2)
	colTypes := []ColumnType{TinyIntColumnType, TinyIntColumnType, IntColumnType, IntColumnType, BigIntColumnType, BigIntColumnType, DoubleColumnType, DoubleColumnType, VarcharColumnType, VarcharColumnType, decType1, decType1}
	rf, err := NewRowsFactory(colTypes)
	require.Nil(t, err)
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
	dec, err := NewDecFromString("12345.54321")
	require.Nil(t, err)
	rows.AppendDecimalToColumn(10, *dec)
	rows.AppendNullToColumn(11)
	row := rows.GetRow(0)
	return &row
}

func TestConstantTinyIntExpression(t *testing.T) {
	row := createRow(t)
	expr, err := NewConstantInt(TinyIntColumnType, 100)
	require.Nil(t, err)
	val, null, err := expr.EvalInt64(row)
	require.Nil(t, err)
	require.False(t, null)
	require.Equal(t, int64(100), val)
}

func TestConstantIntExpression(t *testing.T) {
	row := createRow(t)
	expr, err := NewConstantInt(IntColumnType, 101)
	require.Nil(t, err)
	val, null, err := expr.EvalInt64(row)
	require.Nil(t, err)
	require.False(t, null)
	require.Equal(t, int64(101), val)
}

func TestConstantBigIntExpression(t *testing.T) {
	row := createRow(t)
	expr, err := NewConstantInt(BigIntColumnType, 102)
	require.Nil(t, err)
	val, null, err := expr.EvalInt64(row)
	require.Nil(t, err)
	require.False(t, null)
	require.Equal(t, int64(102), val)
}

func TestConstantDoubleExpression(t *testing.T) {
	row := createRow(t)
	expr, err := NewConstantDouble(DoubleColumnType, 1234.32)
	require.Nil(t, err)
	val, null, err := expr.EvalFloat64(row)
	require.Nil(t, err)
	require.False(t, null)
	require.Equal(t, 1234.32, val)
}

func TestConstantStringExpression(t *testing.T) {
	row := createRow(t)
	expr, err := NewConstantVarchar(VarcharColumnType, "other-string")
	require.Nil(t, err)
	val, null, err := expr.EvalString(row)
	require.Nil(t, err)
	require.False(t, null)
	require.Equal(t, "other-string", val)
}

func TestScalarFunctionExpression(t *testing.T) {
	row := createRow(t)
	colExpr1, err := NewColumnExpression(0, TinyIntColumnType)
	require.Nil(t, err)
	colExpr2, err := NewColumnExpression(2, IntColumnType)
	require.Nil(t, err)
	expr1, err := NewScalarFunctionExpression(BigIntColumnType, "gt", colExpr2, colExpr1)
	require.Nil(t, err)
	val, null, err := expr1.EvalInt64(row)
	require.Nil(t, err)
	require.False(t, null)
	require.Equal(t, int64(1), val)
}
