package server

import (
	"github.com/squareup/pranadb/cluster"
	"github.com/stretchr/testify/require"
	"log"
	"testing"

	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/table"
)

func TestCreateMaterializedView(t *testing.T) {
	config := Config{
		NodeID:     1,
		NumShards:  10,
		TestServer: true,
	}
	server, err := NewServer(config)
	require.NoError(t, err)
	err = server.Start()
	require.NoError(t, err)
	ce := server.GetCommandExecutor()

	colTypes := []common.ColumnType{common.BigIntColumnType, common.VarcharColumnType, common.DoubleColumnType}
	pkCols := []int{0}
	_, err = ce.ExecuteSQLStatement("test", `
		create source sensor_readings(
			sensor_id big int,
			location varchar,
			temperature double,
			primary key (sensor_id)
		)
    `)
	require.NoError(t, err)

	_, err = ce.ExecuteSQLStatement("test", `
		create materialized view max_readings
            select sensor_id, max(temperature)
            from test.sensor_readings
			where location='wincanton' group by sensor_id
    `)
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
	require.NoError(t, err)

	mvInfo, ok := server.GetMetaController().GetMaterializedView("test", "max_readings")
	require.True(t, ok)

	expectedColTypes := []common.ColumnType{common.BigIntColumnType, common.DoubleColumnType}
	expectedRf := common.NewRowsFactory(expectedColTypes)
	expectedRows := expectedRf.NewRows(10)
	common.AppendRow(t, expectedRows, expectedColTypes, 1, 25.5)
	expectedRow := expectedRows.GetRow(0)

	waitUntilRowsInTable(t, server.GetCluster(), mvInfo.TableInfo.ID, 1)

	row, err := table.LookupInPk(mvInfo.TableInfo, []interface{}{int64(1)}, pkCols, 9, expectedRf, server.GetCluster())
	require.NoError(t, err)
	require.NotNil(t, row)
	common.RowsEqual(t, expectedRow, *row, expectedColTypes)
}

func TestExecutePullQuery(t *testing.T) {
	config := Config{
		NodeID:     1,
		NumShards:  10,
		TestServer: true,
	}
	server, err := NewServer(config)
	require.NoError(t, err)
	err = server.Start()
	require.NoError(t, err)
	ce := server.GetCommandExecutor()

	colTypes := []common.ColumnType{common.BigIntColumnType, common.VarcharColumnType, common.DoubleColumnType}

	statement := `
		create source sensor_readings(
			sensor_id big int,
			location varchar,
			temperature double,
			primary key (sensor_id)
		)
    `
	_, err = ce.ExecuteSQLStatement("test", statement)
	require.NoError(t, err)

	rf := common.NewRowsFactory(colTypes)
	require.NoError(t, err)
	rows := rf.NewRows(10)
	common.AppendRow(t, rows, colTypes, 1, "wincanton", 25.5)
	// testutils.AppendRow(t, rows, colTypes, 2, "london", 28.1)
	// testutils.AppendRow(t, rows, colTypes, 3, "los angeles", 35.6)
	source, ok := server.GetMetaController().GetSource("test", "sensor_readings")
	require.True(t, ok)
	err = ce.GetPushEngine().IngestRows(rows, source.TableInfo.ID)
	require.NoError(t, err)

	waitUntilRowsInTable(t, server.GetCluster(), source.TableInfo.ID, 1)
	log.Printf("There are %d rows in table %d", 1, source.TableInfo.ID)

	// query := "select location, max(temperature) from test.sensor_readings group by location having location='wincanton'"
	// query := "select sensor_id, location, temperature from test.sensor_readings where location='wincanton'"
	query := "select sensor_id, location, temperature from test.sensor_readings where location='wincanton' order by temperature"
	exec, err := ce.ExecuteSQLStatement("test", query)
	require.NoError(t, err)
	rows, err = exec.GetRows(100)
	require.NoError(t, err)
	require.Equal(t, 1, rows.RowCount())

	expectedRows := rf.NewRows(10)

	common.AppendRow(t, expectedRows, colTypes, 1, "wincanton", 25.5)
	// testutils.AppendRow(t, expectedRows, colTypes, 2, "london", 28.1)
	// testutils.AppendRow(t, expectedRows, colTypes, 3, "los angeles", 35.6)

	expectedRow := expectedRows.GetRow(0)

	actualRow := rows.GetRow(0)

	common.RowsEqual(t, expectedRow, actualRow, colTypes)
}

func waitUntilRowsInTable(t *testing.T, stor cluster.Cluster, tableID uint64, numRows int) {
	t.Helper()
	keyPrefix := make([]byte, 0)
	keyPrefix = common.AppendUint64ToBufferLittleEndian(keyPrefix, tableID)
	common.WaitUntil(t, func() (bool, error) {
		remPairs, err := stor.LocalScan(keyPrefix, keyPrefix, -1)
		if err != nil {
			return false, err
		}
		return numRows == len(remPairs), nil
	})
}
