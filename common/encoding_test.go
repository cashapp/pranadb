package common

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

var singleVarcharColumn = []ColumnType{VarcharColumnType}
var singleIntColumn = []ColumnType{IntColumnType}
var singleFloatColumn = []ColumnType{DoubleColumnType}

func TestEncodeDecodeInt(t *testing.T) {
	rf, err := NewRowsFactory(singleIntColumn)
	require.Nil(t, err)
	encodeDecodeInt(t, rf, 0)
	encodeDecodeInt(t, rf, math.MinInt64)
	encodeDecodeInt(t, rf, math.MaxInt64)
	encodeDecodeInt(t, rf, -1)
	encodeDecodeInt(t, rf, 1)
	encodeDecodeInt(t, rf, -10)
	encodeDecodeInt(t, rf, 10)
}

func TestEncodeDecodeString(t *testing.T) {
	rf, err := NewRowsFactory(singleVarcharColumn)
	require.Nil(t, err)
	encodeDecodeString(t, rf, "")
	encodeDecodeString(t, rf, "zxy123")
	encodeDecodeString(t, rf, "\u2318")
}

func TestEncodeDecodeFloat(t *testing.T) {
	rf, err := NewRowsFactory(singleFloatColumn)
	require.Nil(t, err)
	encodeDecodeFloat(t, rf, 0)
	encodeDecodeFloat(t, rf, -1234.5678)
	encodeDecodeFloat(t, rf, 1234.5678)
	encodeDecodeFloat(t, rf, math.MaxFloat64)
}

func TestEncodeDecodeRow(t *testing.T) {
	decType1 := NewDecimalColumnType(10, 2)
	colTypes := []ColumnType{TinyIntColumnType, IntColumnType, BigIntColumnType, DoubleColumnType, VarcharColumnType, decType1}
	rf, err := NewRowsFactory(colTypes)
	require.Nil(t, err)
	rows := rf.NewRows(10)
	rows.AppendInt64ToColumn(0, 255)
	rows.AppendInt64ToColumn(1, math.MaxInt32)
	rows.AppendInt64ToColumn(2, math.MaxInt64)
	rows.AppendFloat64ToColumn(3, math.MaxFloat64)
	rows.AppendStringToColumn(4, "somestringxyz")
	dec, err := NewDecFromString("12345678.32")
	require.Nil(t, err)
	rows.AppendDecimalToColumn(5, *dec)
	testEncodeDecodeRow(t, rows, colTypes)
}

func TestEncodeDecodeRowWithNulls(t *testing.T) {
	decType1 := NewDecimalColumnType(10, 2)
	colTypes := []ColumnType{TinyIntColumnType, TinyIntColumnType, IntColumnType, IntColumnType, BigIntColumnType, BigIntColumnType, DoubleColumnType, DoubleColumnType, VarcharColumnType, VarcharColumnType, decType1, decType1}
	rf, err := NewRowsFactory(colTypes)
	require.Nil(t, err)
	rows := rf.NewRows(10)
	rows.AppendInt64ToColumn(0, 255)
	rows.AppendNullToColumn(1)
	rows.AppendInt64ToColumn(2, math.MaxInt32)
	rows.AppendNullToColumn(3)
	rows.AppendInt64ToColumn(4, math.MaxInt64)
	rows.AppendNullToColumn(5)
	rows.AppendFloat64ToColumn(6, math.MaxFloat64)
	rows.AppendNullToColumn(7)
	rows.AppendStringToColumn(8, "somestringxyz")
	rows.AppendNullToColumn(9)
	dec, err := NewDecFromString("12345678.32")
	require.Nil(t, err)
	rows.AppendDecimalToColumn(10, *dec)
	rows.AppendNullToColumn(11)
	testEncodeDecodeRow(t, rows, colTypes)
}

func testEncodeDecodeRow(t *testing.T, rows *Rows, colTypes []ColumnType) {
	t.Helper()
	row := rows.GetRow(0)
	var buffer []byte
	buff, err := EncodeRow(&row, colTypes, buffer)
	require.Nil(t, err)
	err = DecodeRow(buff, colTypes, rows)
	require.Nil(t, err)
	actualRow := rows.GetRow(1)
	RowsEqual(t, row, actualRow, colTypes)
}

func TestEncodeDecodeDecimal(t *testing.T) {
	colTypes := []ColumnType{NewDecimalColumnType(10, 2)}
	rf, err := NewRowsFactory(colTypes)
	require.Nil(t, err)
	dec, err := NewDecFromString("0.00")
	require.Nil(t, err)
	encodeDecodeDecimal(t, rf, *dec, colTypes)
	dec, err = NewDecFromString("-12345678.12")
	require.Nil(t, err)
	encodeDecodeDecimal(t, rf, *dec, colTypes)
	dec, err = NewDecFromString("12345678.12")
	require.Nil(t, err)
	encodeDecodeDecimal(t, rf, *dec, colTypes)
}

func encodeDecodeInt(t *testing.T, rf *RowsFactory, val int64) {
	t.Helper()
	rows := rf.NewRows(1)
	rows.AppendInt64ToColumn(0, val)
	encodeDecode(t, rows, singleIntColumn)
}

func encodeDecodeString(t *testing.T, rf *RowsFactory, val string) {
	t.Helper()
	rows := rf.NewRows(1)
	rows.AppendStringToColumn(0, val)
	encodeDecode(t, rows, singleVarcharColumn)
}

func encodeDecodeFloat(t *testing.T, rf *RowsFactory, val float64) {
	t.Helper()
	rows := rf.NewRows(1)
	rows.AppendFloat64ToColumn(0, val)
	encodeDecode(t, rows, singleFloatColumn)
}

func encodeDecodeDecimal(t *testing.T, rf *RowsFactory, val Decimal, colTypes []ColumnType) {
	t.Helper()
	rows := rf.NewRows(1)
	rows.AppendDecimalToColumn(0, val)
	encodeDecode(t, rows, colTypes)
}

func encodeDecode(t *testing.T, rows *Rows, columnTypes []ColumnType) {
	t.Helper()
	row := rows.GetRow(0)
	var buffer []byte
	buffer, err := EncodeRow(&row, columnTypes, buffer)
	require.Nil(t, err)
	err = DecodeRow(buffer, columnTypes, rows)
	require.Nil(t, err)

	row1 := rows.GetRow(0)
	row2 := rows.GetRow(1)

	RowsEqual(t, row1, row2, columnTypes)
}

func TestIsLittleEndian(t *testing.T) {
	require.True(t, IsLittleEndian)
}

func TestEncodeDecodeUint64sLittleEndianArch(t *testing.T) {
	IsLittleEndian = true
	testEncodeDecodeUint64s(t, 0, 1, math.MaxUint64, 12345678)
}

func TestEncodeDecodeUint64sBigEndianArch(t *testing.T) {
	IsLittleEndian = false
	testEncodeDecodeUint64s(t, 0, 1, math.MaxUint64, 12345678)
}

func testEncodeDecodeUint64s(t *testing.T, vals ...uint64) {
	t.Helper()
	for _, val := range vals {
		testEncodeDecodeUint64(t, val)
	}
}

func testEncodeDecodeUint64(t *testing.T, val uint64) {
	t.Helper()
	buff := make([]byte, 0, 8)
	buff = AppendUint64ToBufferLittleEndian(buff, val)
	valRead := ReadUint64FromBufferLittleEndian(buff, 0)
	require.Equal(t, val, valRead)
}

func TestEncodeDecodeUint32sLittleEndianArch(t *testing.T) {
	IsLittleEndian = true
	testEncodeDecodeUint32s(t, 0, 1, math.MaxUint32, 12345678)
}

func TestEncodeDecodeUint32sBigEndianArch(t *testing.T) {
	IsLittleEndian = false
	testEncodeDecodeUint32s(t, 0, 1, math.MaxUint32, 12345678)
}

func testEncodeDecodeUint32s(t *testing.T, vals ...uint32) {
	t.Helper()
	for _, val := range vals {
		testEncodeDecodeUint32(t, val)
	}
}

func testEncodeDecodeUint32(t *testing.T, val uint32) {
	t.Helper()
	buff := make([]byte, 0, 4)
	buff = AppendUint32ToBufferLittleEndian(buff, val)
	valRead := ReadUint32FromBufferLittleEndian(buff, 0)
	require.Equal(t, val, valRead)
}
