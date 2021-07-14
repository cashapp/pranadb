package server

import (
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"testing"
	"time"

	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/table"
)

const (
	testReplicationFactor = 3
)

func TestCreateMaterializedViewClustered(t *testing.T) {

	servers := startCluster(t)
	defer stopCluster(t, servers)
	server := servers[0]
	ce := server.GetCommandExecutor()
	colTypes := []common.ColumnType{common.BigIntColumnType, common.VarcharColumnType, common.DoubleColumnType}

	_, err := ce.ExecuteSQLStatement("test", `
		create source latest_sensor_readings(
			sensor_id big int,
			location varchar,
			temperature double,
			primary key (sensor_id)
		)
    `)
	require.NoError(t, err)

	_, err = ce.ExecuteSQLStatement("test", `
		create materialized view latest_wincanton_readings
            select sensor_id, temperature
            from test.latest_sensor_readings
			where location='wincanton'
    `)
	require.NoError(t, err)

	rf := common.NewRowsFactory(colTypes)
	require.NoError(t, err)

	rows := rf.NewRows(10)

	common.AppendRow(t, rows, colTypes, 1, "wincanton", 25.5)
	common.AppendRow(t, rows, colTypes, 2, "london", 28.1)
	common.AppendRow(t, rows, colTypes, 3, "los angeles", 35.6)

	source, ok := server.GetMetaController().GetSource("test", "latest_sensor_readings")
	require.True(t, ok)
	err = ce.GetPushEngine().IngestRows(rows, source.TableInfo.ID)
	require.NoError(t, err)

	mvInfo, ok := server.GetMetaController().GetMaterializedView("test", "latest_wincanton_readings")
	require.True(t, ok)

	expectedColTypes := []common.ColumnType{common.BigIntColumnType, common.DoubleColumnType}
	expectedRf := common.NewRowsFactory(expectedColTypes)
	expectedRows := expectedRf.NewRows(10)
	common.AppendRow(t, expectedRows, expectedColTypes, 1, 25.5)
	expectedRow := expectedRows.GetRow(0)

	waitUntilRowsInTableClustered(t, servers, mvInfo.TableInfo.ID, 1)

	row := lookupInAllShards(t, []interface{}{int64(1)}, servers, mvInfo.TableInfo, expectedRf)
	require.NotNil(t, row)
	common.RowsEqual(t, expectedRow, *row, expectedColTypes)
}

func TestExecutePullQueryClustered(t *testing.T) {

	servers := startCluster(t)
	defer stopCluster(t, servers)
	server := servers[0]
	err := server.Start()
	require.NoError(t, err)
	ce := server.GetCommandExecutor()
	colTypes := []common.ColumnType{common.BigIntColumnType, common.VarcharColumnType, common.DoubleColumnType}

	_, err = ce.ExecuteSQLStatement("test", `
		create source latest_sensor_readings(
			sensor_id big int,
			location varchar,
			temperature double,
			primary key (sensor_id)
		)
    `)
	require.NoError(t, err)

	rf := common.NewRowsFactory(colTypes)
	require.NoError(t, err)
	rows := rf.NewRows(10)
	common.AppendRow(t, rows, colTypes, 1, "wincanton", 25.5)
	common.AppendRow(t, rows, colTypes, 2, "london", 28.1)
	common.AppendRow(t, rows, colTypes, 3, "los angeles", 35.6)
	source, ok := server.GetMetaController().GetSource("test", "latest_sensor_readings")
	require.True(t, ok)
	err = ce.GetPushEngine().IngestRows(rows, source.TableInfo.ID)
	require.NoError(t, err)

	waitUntilRowsInTable(t, server.GetCluster(), source.TableInfo.ID, 3)

	query := "select sensor_id, location, temperature from test.latest_sensor_readings"
	exec, err := ce.ExecuteSQLStatement("test", query)
	require.NoError(t, err)
	rows, err = exec.GetRows(100)
	require.NoError(t, err)
	require.Equal(t, 3, rows.RowCount())

	expectedRows := rf.NewRows(10)

	common.AppendRow(t, expectedRows, colTypes, 1, "wincanton", 25.5)
	common.AppendRow(t, expectedRows, colTypes, 2, "london", 28.1)
	common.AppendRow(t, expectedRows, colTypes, 3, "los angeles", 35.6)

	rExpected := common.SortRows(common.RowsToSlice(expectedRows))
	rActual := common.SortRows(common.RowsToSlice(rows))

	for i := 0; i < len(rExpected); i++ {
		expected := rExpected[i]
		actual := rActual[i]
		common.RowsEqual(t, *expected, *actual, rf.ColumnTypes)
	}
}

func startCluster(t *testing.T) []*Server {
	t.Helper()
	numServers := 3
	servers := make([]*Server, numServers)
	nodeAddresses := []string{
		"localhost:63001",
		"localhost:63002",
		"localhost:63003",
	}
	dataDir, err := ioutil.TempDir("", "dragon-test")
	// TODO remove datadir on test stop
	require.NoError(t, err)
	for i := 0; i < numServers; i++ {
		config := Config{
			NodeID:            i,
			NodeAddresses:     nodeAddresses,
			NumShards:         10,
			ReplicationFactor: testReplicationFactor,
			DataDir:           dataDir,
			TestServer:        false,
		}
		s, err := NewServer(config)
		require.NoError(t, err)
		err = s.Start()
		require.NoError(t, err)
		servers[i] = s
		require.NoError(t, err)
	}
	time.Sleep(5 * time.Second)
	return servers
}

func stopCluster(t *testing.T, servers []*Server) {
	t.Helper()
	for _, server := range servers {
		err := server.Stop()
		require.NoError(t, err)
	}
}

func waitUntilRowsInTableClustered(t *testing.T, servers []*Server, tableID uint64, numRows int) {
	t.Helper()
	keyPrefix := make([]byte, 0)
	keyPrefix = common.AppendUint64ToBufferLittleEndian(keyPrefix, tableID)

	common.WaitUntil(t, func() (bool, error) {
		totRows := 0
		for _, server := range servers {
			remPairs, err := server.cluster.LocalScan(keyPrefix, keyPrefix, -1)
			if err != nil {
				return false, err
			}
			totRows += len(remPairs)
		}
		// The rows will be replicated across all nodes
		return numRows*testReplicationFactor == totRows, nil
	})
}

func lookupInAllShards(t *testing.T, key []interface{}, servers []*Server, tableInfo *common.TableInfo, factory *common.RowsFactory) *common.Row {
	t.Helper()
	var foundRow *common.Row
	foundCount := 0
	for _, server := range servers {
		found := false
		// Row should only exist max once in each server
		for _, shardID := range server.GetCluster().GetAllShardIDs() {
			row, err := table.LookupInPk(tableInfo, key, tableInfo.PrimaryKeyCols, shardID, factory, server.GetCluster())
			require.NoError(t, err)
			if row != nil {
				require.False(t, found, "row already exists in server")
				if foundRow != nil {
					common.RowsEqual(t, *foundRow, *row, factory.ColumnTypes)
				} else {
					foundRow = row
				}
				found = true
				foundCount++
			}
		}
	}
	// Row should be in <replication_factor> servers
	require.Equal(t, testReplicationFactor, foundCount)
	return foundRow
}
