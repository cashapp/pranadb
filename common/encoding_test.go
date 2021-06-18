package common

import (
	"github.com/stretchr/testify/require"
	"math"
	"testing"
)

var singleVarcharColumn = []ColumnType{TypeVarchar}
var singleIntColumn = []ColumnType{TypeBigInt}

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

func encodeDecodeInt(t *testing.T, rf *RowsFactory, val int64) {
	rows := rf.NewRows(1)
	rows.AppendInt64ToColumn(0, val)
	encodeDecode(t, rows, singleIntColumn)
}

func encodeDecodeString(t *testing.T, rf *RowsFactory, val string) {
	rows := rf.NewRows(1)
	rows.AppendStringToColumn(0, val)
	encodeDecode(t, rows, singleVarcharColumn)
}

func encodeDecode(t *testing.T, rows *PushRows, columnTypes []ColumnType) {
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

func RowsEqual(t *testing.T, expected *PushRow, actual *PushRow, colTypes []ColumnType) {
	require.Equal(t, expected.ColCount(), actual.ColCount())
	for colIndex, colType := range colTypes {
		switch colType {
		case TypeTinyInt, TypeInt, TypeBigInt:
			val1 := expected.GetInt64(colIndex)
			val2 := actual.GetInt64(colIndex)
			require.Equal(t, val1, val2)
		case TypeDecimal:
			// TODO
		case TypeDouble:
			val1 := expected.GetFloat64(colIndex)
			val2 := actual.GetFloat64(colIndex)
			require.Equal(t, val1, val2)
		case TypeVarchar:
			val1 := expected.GetString(colIndex)
			val2 := actual.GetString(colIndex)
			require.Equal(t, val1, val2)
		default:
			t.Errorf("unexpected column type %d", colType)
		}
	}
}
