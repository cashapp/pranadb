package common

import (
	"github.com/stretchr/testify/require"
	"sort"
	"testing"
	"time"
)

func SortRows(rows []*Row) []*Row {
	sort.SliceStable(rows, func(i, j int) bool {
		return rows[i].GetInt64(0) < rows[j].GetInt64(0)
	})
	return rows
}

func RowsToSlice(rows *Rows) []*Row {
	slice := make([]*Row, rows.RowCount())
	for i := 0; i < rows.RowCount(); i++ {
		row := rows.GetRow(i)
		slice[i] = &row
	}
	return slice
}

func AllRowsEqual(t *testing.T, expected *Rows, actual *Rows, colTypes []ColumnType) {
	t.Helper()
	require.Equal(t, expected.RowCount(), actual.RowCount())
	for i := 0; i < expected.RowCount(); i++ {
		RowsEqual(t, expected.GetRow(i), actual.GetRow(i), colTypes)
	}
}

func RowsEqual(t *testing.T, expected Row, actual Row, colTypes []ColumnType) {
	t.Helper()
	require.Equal(t, expected.ColCount(), actual.ColCount())
	for colIndex, colType := range colTypes {
		expectedNull := expected.IsNull(colIndex)
		actualNull := actual.IsNull(colIndex)
		require.Equal(t, expectedNull, actualNull)
		if !expectedNull {
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
}

func AppendRow(t *testing.T, rows *Rows, colTypes []ColumnType, colVals ...interface{}) {
	t.Helper()
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
			require.NoError(t, err)
			rows.AppendDecimalToColumn(i, *dec)
		default:
			panic(colType.Type)
		}
	}
}

type Predicate func() (bool, error)

func WaitUntil(t *testing.T, predicate Predicate) {
	t.Helper()
	WaitUntilWithDur(t, predicate, 10*time.Second)
}

func WaitUntilWithDur(t *testing.T, predicate Predicate, timeout time.Duration) {
	t.Helper()
	complete, err := WaitUntilWithError(predicate, timeout)
	require.NoError(t, err)
	require.True(t, complete, "timed out waiting for predicate")
}

func WaitUntilWithError(predicate Predicate, timeout time.Duration) (bool, error) {
	start := time.Now()
	for {
		complete, err := predicate()
		if err != nil {
			return false, err
		}
		if complete {
			return true, nil
		}
		time.Sleep(time.Millisecond)
		if time.Now().Sub(start) >= timeout {
			return false, nil
		}
	}
}
