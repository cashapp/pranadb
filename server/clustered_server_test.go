package server

import (
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"log"
	"testing"
	"time"

	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/table"
)

const (
	testReplicationFactor = 3
)

func startCluster(t *testing.T) []*Server {
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
	for _, server := range servers {
		err := server.Stop()
		require.NoError(t, err)
	}
}

func TestCreateMaterializedViewClustered(t *testing.T) {

	servers := startCluster(t)

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

func waitUntilRowsInTableClustered(t *testing.T, servers []*Server, tableID uint64, numRows int) {
	t.Helper()
	keyPrefix := make([]byte, 0)
	keyPrefix = common.AppendUint64ToBufferLittleEndian(keyPrefix, tableID)
	log.Printf("Waiting until there are %d rows in table %d", numRows, tableID)

	common.WaitUntil(t, func() (bool, error) {
		totRows := 0
		for _, server := range servers {
			log.Printf("Scanning in server with node id %d", server.GetCluster().GetNodeID())
			remPairs, err := server.cluster.LocalScan(keyPrefix, keyPrefix, -1)
			if err != nil {
				return false, err
			}
			log.Printf("Found %d rows", len(remPairs))
			totRows += len(remPairs)
		}
		log.Printf("Num rows is %d tot rows is %d num servers is %d", numRows, totRows, len(servers))

		// The rows will be replicated across all nodes
		return numRows*testReplicationFactor == totRows, nil
	})
}

func lookupInAllShards(t *testing.T, key []interface{}, servers []*Server, tableInfo *common.TableInfo, factory *common.RowsFactory) *common.Row {
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

func TestExecutePullQueryClustered(t *testing.T) {

	servers := startCluster(t)

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
	log.Printf("There are %d rows in table %d", 1, source.TableInfo.ID)

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

	for i := 0; i < expectedRows.RowCount(); i++ {
		expected := expectedRows.GetRow(0)
		actual := rows.GetRow(0)
		common.RowsEqual(t, expected, actual, rf.ColumnTypes)
	}

}
