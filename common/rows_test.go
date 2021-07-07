package common

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestRows(t *testing.T) {
	decType1 := NewDecimalColumnType(10, 2)
	colTypes := []ColumnType{TinyIntColumnType, IntColumnType, BigIntColumnType, DoubleColumnType, VarcharColumnType, decType1}
	rf, err := NewRowsFactory(colTypes)
	require.Nil(t, err)
	rows := rf.NewRows(1)
	rowCount := 10
	for i := 0; i < rowCount; i++ {
		if useNull(i, 0) {
			rows.AppendNullToColumn(0)
		} else {
			rows.AppendInt64ToColumn(0, tinyIntVal(i))
		}
		if useNull(i, 1) {
			rows.AppendNullToColumn(1)
		} else {
			rows.AppendInt64ToColumn(1, intVal(i))
		}
		if useNull(i, 2) {
			rows.AppendNullToColumn(2)
		} else {
			rows.AppendInt64ToColumn(2, bigIntVal(i))
		}
		if useNull(i, 3) {
			rows.AppendNullToColumn(3)
		} else {
			rows.AppendFloat64ToColumn(3, floatVal(i))
		}
		if useNull(i, 4) {
			rows.AppendNullToColumn(4)
		} else {
			rows.AppendStringToColumn(4, stringVal(i))
		}
		if useNull(i, 5) {
			rows.AppendNullToColumn(5)
		} else {
			rows.AppendDecimalToColumn(5, decVal(t, i))
		}
	}
	require.Equal(t, rowCount, rows.RowCount())
	for i := 0; i < 10; i++ {
		row := rows.GetRow(i)
		require.NotNil(t, row)
		if useNull(i, 0) {
			require.True(t, row.IsNull(0))
		} else {
			require.False(t, row.IsNull(0))
			require.Equal(t, tinyIntVal(i), row.GetInt64(0))
		}
		if useNull(i, 1) {
			require.True(t, row.IsNull(1))
		} else {
			require.False(t, row.IsNull(1))
			require.Equal(t, intVal(i), row.GetInt64(1))
		}
		if useNull(i, 2) {
			require.True(t, row.IsNull(2))
		} else {
			require.False(t, row.IsNull(2))
			require.Equal(t, bigIntVal(i), row.GetInt64(2))
		}
		if useNull(i, 3) {
			require.True(t, row.IsNull(3))
		} else {
			require.False(t, row.IsNull(3))
			require.Equal(t, floatVal(i), row.GetFloat64(3))
		}
		if useNull(i, 4) {
			require.True(t, row.IsNull(4))
		} else {
			require.False(t, row.IsNull(4))
			require.Equal(t, stringVal(i), row.GetString(4))
		}
		if useNull(i, 5) {
			require.True(t, row.IsNull(5))
		} else {
			require.False(t, row.IsNull(5))
			expectedDec := decVal(t, i)
			actualDec := row.GetDecimal(5)
			require.Equal(t, expectedDec.ToString(), actualDec.ToString())
		}
	}
}

func useNull(rowIndex int, colIndex int) bool {
	return ((rowIndex*colIndex)+colIndex)%2 == 0
}

func tinyIntVal(rowIndex int) int64 {
	return int64(rowIndex)
}

func intVal(rowIndex int) int64 {
	return int64(rowIndex) + 1
}

func bigIntVal(rowIndex int) int64 {
	return int64(rowIndex) + 2
}

func floatVal(rowIndex int) float64 {
	return float64(rowIndex) + 1.1
}

func stringVal(rowIndex int) string {
	return fmt.Sprintf("aardvarks-%d", rowIndex)
}

func decVal(t *testing.T, rowIndex int) Decimal {
	dec, err := NewDecFromFloat64(10000 * floatVal(rowIndex))
	require.Nil(t, err)
	return *dec
}
