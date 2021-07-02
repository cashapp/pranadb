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

	RowsEqual(t, &row1, &row2, columnTypes)
}

func RowsEqual(t *testing.T, expected *Row, actual *Row, colTypes []ColumnType) {
	t.Helper()
	require.Equal(t, expected.ColCount(), actual.ColCount())
	for colIndex, colType := range colTypes {
		switch colType.TypeNumber {
		case TypeTinyInt, TypeInt, TypeBigInt:
			val1 := expected.GetInt64(colIndex)
			val2 := actual.GetInt64(colIndex)
			require.Equal(t, val1, val2)
		case TypeDecimal:
			// TODO
		case TypeDouble:
			val1 := expected.GetFloat64(colIndex)
			val2 := actual.GetFloat64(colIndex)
			require.InDelta(t, val1, val2, 0.0001)
		case TypeVarchar:
			val1 := expected.GetString(colIndex)
			val2 := actual.GetString(colIndex)
			require.Equal(t, val1, val2)
		default:
			t.Errorf("unexpected column type %d", colType)
		}
	}
}
