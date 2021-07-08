package server

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/table"
)

func TestCreateMaterializedView(t *testing.T) {
	nodeID := 1
	server := NewServer(nodeID, 10)
	err := server.Start()
	require.Nil(t, err)
	ce := server.GetCommandExecutor()

	colTypes := []common.ColumnType{common.BigIntColumnType, common.VarcharColumnType, common.DoubleColumnType}
	pkCols := []int{0}
	err = ce.CreateSource("test", "sensor_readings", []string{"sensor_id", "location", "temperature"}, colTypes, pkCols, nil)
	require.Nil(t, err)

	query := "select sensor_id, max(temperature) from test.sensor_readings where location='wincanton' group by sensor_id"
	err = ce.CreateMaterializedView("test", "max_readings", query)
	require.NoError(t, err)

	rf := common.NewRowsFactory(colTypes)
	require.NoError(t, err)

	rows := rf.NewRows(10)

	common.AppendRow(t, rows, colTypes, 1, "wincanton", 25.5)
	common.AppendRow(t, rows, colTypes, 2, "london", 28.1)
	common.AppendRow(t, rows, colTypes, 3, "los angeles", 35.6)

	source, ok := server.GetMetaController().GetSource("test", "sensor_readings")
	require.True(t, ok)
	err = ce.GetPushEngine().IngestRows(rows, source.TableInfo.ID)
	require.Nil(t, err)

	time.Sleep(5 * time.Second)

	mvInfo, ok := server.GetMetaController().GetMaterializedView("test", "max_readings")
	require.True(t, ok)

	expectedColTypes := []common.ColumnType{common.BigIntColumnType, common.DoubleColumnType}
	expectedRf := common.NewRowsFactory(expectedColTypes)
	expectedRows := expectedRf.NewRows(10)
	common.AppendRow(t, expectedRows, expectedColTypes, 1, 25.5)
	expectedRow := expectedRows.GetRow(0)

	row, err := table.LookupInPk(mvInfo.TableInfo, []interface{}{int64(1)}, pkCols, 9, expectedRf, server.GetStorage())
	require.NoError(t, err)
	require.NotNil(t, row)
	common.RowsEqual(t, expectedRow, *row, expectedColTypes)
}

func TestExecutePullQuery(t *testing.T) {
	nodeID := 1
	server := NewServer(nodeID, 10)
	err := server.Start()
	require.Nil(t, err)
	ce := server.GetCommandExecutor()

	colTypes := []common.ColumnType{common.BigIntColumnType, common.VarcharColumnType, common.DoubleColumnType}

	err = ce.CreateSource("test", "sensor_readings", []string{"sensor_id", "location", "temperature"}, colTypes, []int{0}, nil)
	require.NoError(t, err)

	rf := common.NewRowsFactory(colTypes)
	require.Nil(t, err)
	rows := rf.NewRows(10)
	common.AppendRow(t, rows, colTypes, 1, "wincanton", 25.5)
	// testutils.AppendRow(t, rows, colTypes, 2, "london", 28.1)
	// testutils.AppendRow(t, rows, colTypes, 3, "los angeles", 35.6)
	source, ok := server.GetMetaController().GetSource("test", "sensor_readings")
	require.True(t, ok)
	err = ce.GetPushEngine().IngestRows(rows, source.TableInfo.ID)
	require.Nil(t, err)

	time.Sleep(5 * time.Second)

	// query := "select location, max(temperature) from test.sensor_readings group by location having location='wincanton'"
	// query := "select sensor_id, location, temperature from test.sensor_readings where location='wincanton'"
	query := "select sensor_id, location, temperature from test.sensor_readings where location='wincanton'"
	exec, err := ce.ExecuteSQLStatement("test", query)
	require.Nil(t, err)
	rows, err = exec.GetRows(100)
	require.Nil(t, err)
	require.Equal(t, 1, rows.RowCount())

	expectedRows := rf.NewRows(10)

	common.AppendRow(t, expectedRows, colTypes, 1, "wincanton", 25.5)
	// testutils.AppendRow(t, expectedRows, colTypes, 2, "london", 28.1)
	// testutils.AppendRow(t, expectedRows, colTypes, 3, "los angeles", 35.6)

	expectedRow := expectedRows.GetRow(0)

	actualRow := rows.GetRow(0)

	common.RowsEqual(t, expectedRow, actualRow, colTypes)
}
