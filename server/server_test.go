package server

import (
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/table"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestCreateMaterializedView(t *testing.T) {
	nodeID := 1

	server := NewServer(nodeID)
	err := server.Start()
	require.Nil(t, err)

	ce := server.GetCommandExecutor()

	colTypes := []common.ColumnType{common.BigIntColumnType, common.VarcharColumnType, common.DoubleColumnType}
	pkCols := []int{0}
	err = ce.CreateSource("test", "sensor_readings", []string{"sensor_id", "location", "temperature"}, colTypes, pkCols, nil)
	require.Nil(t, err)

	query := "select sensor_id, max(temperature) from test.sensor_readings where location='wincanton' group by sensor_id"
	err = ce.CreateMaterializedView("test", "max_readings", query)
	require.Nil(t, err)

	rf, err := common.NewRowsFactory(colTypes)
	require.Nil(t, err)

	rows := rf.NewRows(10)

	appendRow(t, rows, colTypes, 1, "wincanton", 25.5)
	//appendRow(t, rows, colTypes, 2, "london", 28.1)
	//appendRow(t, rows, colTypes, 3, "los angeles", 35.6)

	source, ok := server.GetMetaController().GetSource("test", "sensor_readings")
	require.True(t, ok)

	err = ce.GetPushEngine().IngestRows(source.TableInfo.ID, rows)
	require.Nil(t, err)

	time.Sleep(5 * time.Second)

	mvInfo, ok := server.GetMetaController().GetMaterializedView("test", "max_readings")
	require.True(t, ok)

	expectedColTypes := []common.ColumnType{common.BigIntColumnType, common.DoubleColumnType}
	expectedRf, err := common.NewRowsFactory(expectedColTypes)
	require.Nil(t, err)
	expectedRows := expectedRf.NewRows(10)
	appendRow(t, expectedRows, expectedColTypes, 1, 25.5)
	expectedRow := expectedRows.GetRow(0)

	row, err := table.LookupInPk(mvInfo.TableInfo, []interface{}{int64(1)}, pkCols, 9, rf, server.GetStorage())
	require.Nil(t, err)
	require.NotNil(t, row)
	RowsEqual(t, &expectedRow, row, expectedColTypes)
}

//func TestExecutePullQuery(t *testing.T) {
//	nodeID := 1
//	store := storage.NewFakeStorage(nodeID, 10)
//	clusterMgr := cluster.NewFakeClusterManager(nodeID, 10)
//	prana, err := NewPranaNode(store, clusterMgr, nodeID)
//	require.Nil(t, err)
//
//	colTypes := []common.ColumnType{common.BigIntColumnType, common.VarcharColumnType, common.DoubleColumnType}
//
//	err = prana.CreateSource("test", "sensor_readings", []string{"sensor_id", "location", "temperature"}, colTypes, []int{0}, nil)
//	require.Nil(t, err)
//
//	//query := "select location, max(temperature) from test.sensor_readings group by location having location='wincanton'"
//	query := "select 1, res.* from (select sensor_id, location, temperature from test.sensor_readings union all select sensor_id +1, location, temperature from test.sensor_readings) res"
//	_, err = prana.ExecutePullQuery("test", query)
//	require.Nil(t, err)
//}

func appendRow(t *testing.T, rows *common.Rows, colTypes []common.ColumnType, colVals ...interface{}) {
	require.Equal(t, len(colVals), len(colTypes))

	for i, colType := range colTypes {
		colVal := colVals[i]
		switch colType.TypeNumber {
		case common.TypeTinyInt, common.TypeInt, common.TypeBigInt:
			rows.AppendInt64ToColumn(i, int64(colVal.(int)))
		case common.TypeDouble:
			rows.AppendFloat64ToColumn(i, colVal.(float64))
		case common.TypeVarchar:
			rows.AppendStringToColumn(i, colVal.(string))
		}
	}
}

func RowsEqual(t *testing.T, expected *common.Row, actual *common.Row, colTypes []common.ColumnType) {
	require.Equal(t, expected.ColCount(), actual.ColCount())
	for colIndex, colType := range colTypes {
		switch colType.TypeNumber {
		case common.TypeTinyInt, common.TypeInt, common.TypeBigInt:
			val1 := expected.GetInt64(colIndex)
			val2 := actual.GetInt64(colIndex)
			require.Equal(t, val1, val2)
		case common.TypeDecimal:
			// TODO
		case common.TypeDouble:
			val1 := expected.GetFloat64(colIndex)
			val2 := actual.GetFloat64(colIndex)
			require.Equal(t, val1, val2)
		case common.TypeVarchar:
			val1 := expected.GetString(colIndex)
			val2 := actual.GetString(colIndex)
			require.Equal(t, val1, val2)
		default:
			t.Errorf("unexpected column type %d", colType)
		}
	}
}
