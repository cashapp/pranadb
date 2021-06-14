package sql

import (
	"github.com/squareup/pranadb/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestCreateMaterializedView(t *testing.T) {
	store := storage.NewFakeStorage()
	mgr := NewManager(store)

	colTypes := []ColumnType{TypeBigInt, TypeVarchar, TypeDouble}

	mgr.CreateSource("test", "sensor_readings", []string{"sensor_id", "location", "temperature"}, colTypes, []int{0}, 1, nil)

	sql := "select sensor_id, location from test.sensor_readings where temperature > 32"
	err := mgr.CreateMaterializedView("test", "hot_temps", sql, 1)
	require.Nil(t, err)

	rf, err := NewRowsFactory(colTypes)
	require.Nil(t, err)

	rows := rf.NewRows(10)

	appendRow(t, rows, colTypes, 1, "wincanton", 25.5)
	appendRow(t, rows, colTypes, 2, "london", 28.1)
	appendRow(t, rows, colTypes, 3, "los angeles", 35.6)

	mgrInternal := mgr.(*manager)
	source, ok := mgrInternal.getSource("test", "sensor_readings")
	require.True(t, ok)
	sourceExecutor := source.TableExecutor

	err = sourceExecutor.forwardToConsumingNodes(rows, 1)
	require.Nil(t, err)

	mv, ok := mgrInternal.getMaterializedView("test", "hot_temps")
	require.True(t, ok)

	mvNode := mv.TableExecutor
	table := mvNode.table

	expectedColTypes := []ColumnType{TypeBigInt, TypeVarchar}
	expectedRf, err := NewRowsFactory(expectedColTypes)
	require.Nil(t, err)
	expectedRows := expectedRf.NewRows(10)
	appendRow(t, expectedRows, expectedColTypes, 1, "wincanton")
	expectedRow := expectedRows.GetRow(0)

	row, err := table.LookupInPk([]interface{}{int64(1)}, 1)
	require.Nil(t, err)
	require.NotNil(t, row)
	RowsEqual(t, &expectedRow, row, expectedColTypes)
}

func appendRow(t *testing.T, rows *PullRows, colTypes []ColumnType, colVals ...interface{}) {
	require.Equal(t, len(colVals), len(colTypes))

	for i, colType := range colTypes {
		colVal := colVals[i]
		switch colType {
		case TypeTinyInt, TypeInt, TypeBigInt:
			rows.AppendInt64ToColumn(i, int64(colVal.(int)))
		case TypeDouble:
			rows.AppendFloat64ToColumn(i, colVal.(float64))
		case TypeVarchar:
			rows.AppendStringToColumn(i, colVal.(string))
		}
	}
}
