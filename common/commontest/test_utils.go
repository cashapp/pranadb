package commontest

import (
	"sort"
	"testing"
	"time"

	"github.com/squareup/pranadb/common"
	"github.com/stretchr/testify/require"
)

// Test utils
// I would like these to live in a xxx_test.go file so they're not compiled into the executable however I haven't
// been able to figure out how to do that and still be able to include them in tests from other packages

func SortRows(rows []*common.Row) []*common.Row {
	sort.SliceStable(rows, func(i, j int) bool {
		return rows[i].GetInt64(0) < rows[j].GetInt64(0)
	})
	return rows
}

func RowsToSlice(rows *common.Rows) []*common.Row {
	slice := make([]*common.Row, rows.RowCount())
	for i := 0; i < rows.RowCount(); i++ {
		row := rows.GetRow(i)
		slice[i] = &row
	}
	return slice
}

func AllRowsEqual(t *testing.T, expected *common.Rows, actual *common.Rows, colTypes []common.ColumnType) {
	t.Helper()
	require.Equal(t, expected.RowCount(), actual.RowCount())
	for i := 0; i < expected.RowCount(); i++ {
		RowsEqual(t, expected.GetRow(i), actual.GetRow(i), colTypes)
	}
}

func RowsEqual(t *testing.T, expected common.Row, actual common.Row, colTypes []common.ColumnType) {
	t.Helper()
	require.Equal(t, expected.ColCount(), actual.ColCount())
	for colIndex, colType := range colTypes {
		expectedNull := expected.IsNull(colIndex)
		actualNull := actual.IsNull(colIndex)
		require.Equal(t, expectedNull, actualNull)
		if !expectedNull {
			switch colType.Type {
			case common.TypeTinyInt, common.TypeInt, common.TypeBigInt:
				val1 := expected.GetInt64(colIndex)
				val2 := actual.GetInt64(colIndex)
				require.Equal(t, val1, val2)
			case common.TypeDecimal:
				val1 := expected.GetDecimal(colIndex)
				val2 := actual.GetDecimal(colIndex)
				require.Equal(t, val1.String(), val2.String())
			case common.TypeDouble:
				val1 := expected.GetFloat64(colIndex)
				val2 := actual.GetFloat64(colIndex)
				require.Equal(t, val1, val2)
			case common.TypeVarchar:
				val1 := expected.GetString(colIndex)
				val2 := actual.GetString(colIndex)
				require.Equal(t, val1, val2)
			case common.TypeTimestamp:
				val1 := expected.GetTimestamp(colIndex)
				val2 := actual.GetTimestamp(colIndex)
				require.Equalf(t, 0, val1.Compare(val2), "timestamps not equal: %v and %v", val1, val2)
			default:
				t.Errorf("unexpected column type %d", colType)
			}
		}
	}
}

func AppendRow(t *testing.T, rows *common.Rows, colTypes []common.ColumnType, colVals ...interface{}) {
	t.Helper()
	require.Equal(t, len(colVals), len(colTypes))

	for i, colType := range colTypes {
		colVal := colVals[i]
		if colVal == nil {
			rows.AppendNullToColumn(i)
		} else {
			switch colType.Type {
			case common.TypeTinyInt, common.TypeInt, common.TypeBigInt:
				rows.AppendInt64ToColumn(i, int64(colVal.(int)))
			case common.TypeDouble:
				rows.AppendFloat64ToColumn(i, colVal.(float64))
			case common.TypeVarchar:
				rows.AppendStringToColumn(i, colVal.(string))
			case common.TypeDecimal:
				dec, err := common.NewDecFromString(colVal.(string))
				require.NoError(t, err)
				rows.AppendDecimalToColumn(i, *dec)
			case common.TypeTimestamp:
				ts := common.NewTimestampFromString(colVal.(string))
				rows.AppendTimestampToColumn(i, ts)
			default:
				panic(colType.Type)
			}
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
	complete, err := WaitUntilWithError(predicate, timeout, time.Millisecond)
	require.NoError(t, err)
	require.True(t, complete, "timed out waiting for predicate")
}

func WaitUntilWithError(predicate Predicate, timeout time.Duration, sleepTime time.Duration) (bool, error) {
	start := time.Now()
	for {
		complete, err := predicate()
		if err != nil {
			return false, err
		}
		if complete {
			return true, nil
		}
		time.Sleep(sleepTime)
		if time.Now().Sub(start) >= timeout {
			return false, nil
		}
	}
}
