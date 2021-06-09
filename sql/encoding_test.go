package sql

import (
	"github.com/stretchr/testify/require"
	"math"
	"testing"
)

var singleVarcharColumn = []ColumnType{VarcharColumnType}
var singleIntColumn = []ColumnType{BigIntColumnType}

func TestEncodeDecodeInt(t *testing.T) {
	rf, err := NewRowsFactory(singleIntColumn)
	require.Nil(t, err)
	encodeDecodeInt(t, rf,  0)
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

func encodeDecode(t *testing.T, rows *Rows, columnTypes []ColumnType) {
	row := rows.GetRow(0)
	var buffer []byte
	bufPtr, err := EncodeRow(&row, columnTypes, &buffer)
	require.Nil(t, err)
	err = DecodeRow(bufPtr, columnTypes, rows)
	require.Nil(t, err)

	row1 := rows.GetRow(0)
	row2 := rows.GetRow(1)

	RowsEqual(t, &row1, &row2, columnTypes)
}


func RowsEqual(t *testing.T, row1 *Row, row2 *Row, colTypes []ColumnType) {
	require.Equal(t, row1.ColCount(), row2.ColCount())
	for colIndex, colType := range colTypes {
		switch colType.TypeNumber {
		case TinyIntColumnType.TypeNumber, IntColumnType.TypeNumber, BigIntColumnType.TypeNumber:
			val1 := row1.GetInt64(colIndex)
			val2 := row2.GetInt64(colIndex)
			require.Equal(t, val1, val2)
		case DecimalColumnType.TypeNumber:
			// TODO
		case DoubleColumnType.TypeNumber:
			val1 := row1.GetFloat64(colIndex)
			val2 := row2.GetFloat64(colIndex)
			require.Equal(t, val1, val2)
		case VarcharColumnType.TypeNumber:
			val1 := row1.GetString(colIndex)
			val2 := row2.GetString(colIndex)
			require.Equal(t, val1, val2)
		default:
			t.Errorf("unexpected column type %d", colType.TypeNumber)
		}
	}
}

