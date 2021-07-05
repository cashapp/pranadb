package common

import (
	"github.com/stretchr/testify/require"
	"testing"
)

// Test utils

func AppendRow(t *testing.T, rows *Rows, colTypes []ColumnType, colVals ...interface{}) error {

	require.Equal(t, len(colVals), len(colTypes))

	for i, colType := range colTypes {
		colVal := colVals[i]
		switch colType.Type {
		case TypeTinyInt, TypeInt, TypeBigInt:
			rows.AppendInt64ToColumn(i, int64(colVal.(int)))
		case TypeDouble:
			rows.AppendFloat64ToColumn(i, colVal.(float64))
		case TypeVarchar:
			rows.AppendStringToColumn(i, colVal.(string))
		case TypeDecimal:
			dec, err := NewDecFromString(colVal.(string))
			if err != nil {
				return err
			}
			rows.AppendDecimalToColumn(i, *dec)
		}
	}
	return nil
}

func RowsEqual(t *testing.T, expected Row, actual Row, colTypes []ColumnType) {
	t.Helper()
	require.Equal(t, expected.ColCount(), actual.ColCount())
	for colIndex, colType := range colTypes {
		switch colType.Type {
		case TypeTinyInt, TypeInt, TypeBigInt:
			val1 := expected.GetInt64(colIndex)
			val2 := actual.GetInt64(colIndex)
			require.Equal(t, val1, val2)
		case TypeDecimal:
			val1 := expected.GetDecimal(colIndex)
			val2 := actual.GetDecimal(colIndex)
			require.Equal(t, val1.ToString(), val2.ToString())
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
