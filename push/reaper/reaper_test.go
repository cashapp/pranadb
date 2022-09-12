package reaper

import (
	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/cluster/fake"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/push/exec"
	"github.com/squareup/pranadb/table"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

const shardID uint64 = 12345

func init() {
	log.SetLevel(log.DebugLevel)
}

func TestSimple(t *testing.T) {

	retentionDuration := 5 * time.Second
	store := fake.NewFakeCluster(0, 10)
	reaper := NewReaper(store, 10000, shardID)
	reaper.startNoSchedule()
	tableID := uint64(23)
	lastUpdatedIndexID := uint64(24)
	colTypes := []common.ColumnType{common.BigIntColumnType, common.BigIntColumnType}
	tabInfo := common.NewTableInfo(tableID, "test", "test_table", []int{0}, []string{"col0", "last_updated"},
		colTypes, retentionDuration, lastUpdatedIndexID)
	reaper.addTableNoSchedule(tabInfo)
	te := setupTableExecutor(store, tabInfo, retentionDuration, lastUpdatedIndexID)

	numRows := 10
	generateRows(t, numRows, 0, time.Now().UnixMilli(), 0, te, colTypes, store)

	dur, err := reaper.run(false)
	require.NoError(t, err)

	require.LessOrEqual(t, dur, retentionDuration)
	require.Greater(t, dur, int64(float64(retentionDuration)*0.9))

	checkNumRows(t, tableID, lastUpdatedIndexID, 10, store)

	time.Sleep(retentionDuration + 1*time.Millisecond)
	_, err = reaper.run(false)
	require.NoError(t, err)

	checkNumRows(t, tableID, lastUpdatedIndexID, 0, store)

	reaper.Stop()
}

func TestOneTable(t *testing.T) {

	retentionDuration := 5 * time.Second
	store := fake.NewFakeCluster(0, 10)
	reaper := NewReaper(store, 10000, shardID)
	reaper.startNoSchedule()
	tableID := uint64(23)
	lastUpdatedIndexID := uint64(24)
	colTypes := []common.ColumnType{common.BigIntColumnType, common.BigIntColumnType}
	tabInfo := common.NewTableInfo(tableID, "test", "test_table", []int{0}, []string{"col0", "last_updated"},
		colTypes, retentionDuration, lastUpdatedIndexID)
	reaper.addTableNoSchedule(tabInfo)
	te := setupTableExecutor(store, tabInfo, retentionDuration, lastUpdatedIndexID)

	now := time.Now()
	numRows := 10
	// gen 10 rows each one 1 second update time later than the next
	generateRows(t, numRows, 0, now.UnixMilli()-10000, 1000, te, colTypes, store)

	dur, err := reaper.run(false)
	require.NoError(t, err)
	// Dur should be roughly one s
	require.LessOrEqual(t, dur.Milliseconds(), int64(1000))
	require.Greater(t, dur.Milliseconds(), int64(900)) // Margin of error

	// Oldest six should be deleted
	checkNumRows(t, tableID, lastUpdatedIndexID, 4, store)

	dur, err = reaper.run(false)
	require.NoError(t, err)
	require.LessOrEqual(t, dur.Milliseconds(), int64(1000))
	require.Greater(t, dur.Milliseconds(), int64(900)) // Margin of error

	// No more should be deleted
	checkNumRows(t, tableID, lastUpdatedIndexID, 4, store)

	time.Sleep(1 * time.Second)

	dur, err = reaper.run(false)
	require.NoError(t, err)
	require.LessOrEqual(t, dur.Milliseconds(), int64(1000))
	require.Greater(t, dur.Milliseconds(), int64(900)) // Margin of error

	// Another should be deleted
	checkNumRows(t, tableID, lastUpdatedIndexID, 3, store)

	time.Sleep(3 * time.Second)
	_, err = reaper.run(false)
	require.NoError(t, err)

	// All should be gone
	checkNumRows(t, tableID, lastUpdatedIndexID, 0, store)

	reaper.Stop()
}

func TestUpdateRow(t *testing.T) {

	retentionDuration := 5 * time.Second
	store := fake.NewFakeCluster(0, 10)
	reaper := NewReaper(store, 10000, shardID)
	reaper.startNoSchedule()
	tableID := uint64(23)
	lastUpdatedIndexID := uint64(24)
	colTypes := []common.ColumnType{common.BigIntColumnType, common.BigIntColumnType}
	tabInfo := common.NewTableInfo(tableID, "test", "test_table", []int{0}, []string{"col0", "last_updated"},
		colTypes, retentionDuration, lastUpdatedIndexID)
	reaper.addTableNoSchedule(tabInfo)
	te := setupTableExecutor(store, tabInfo, retentionDuration, lastUpdatedIndexID)

	now := time.Now()
	numRows := 5
	// gen 5 rows with time now
	generateRows(t, numRows, 0, now.UnixMilli(), 0, te, colTypes, store)

	dur, err := reaper.run(false)
	require.NoError(t, err)
	// Dur should be roughly 5s
	require.LessOrEqual(t, dur.Milliseconds(), int64(5000))
	require.Greater(t, dur.Milliseconds(), int64(4900)) // Margin of error
	checkNumRows(t, tableID, lastUpdatedIndexID, 5, store)

	// Now we sleep 5 seconds and then update 2 of them to have last update of now to prevent reaping
	time.Sleep(5 * time.Second)

	now = time.Now()
	// Update row with id 1
	generateRows(t, 1, 1, now.UnixMilli(), 0, te, colTypes, store)
	// Update row with id 3
	generateRows(t, 1, 3, now.UnixMilli(), 0, te, colTypes, store)

	_, err = reaper.run(false)
	require.NoError(t, err)

	// Three should be deleted
	checkNumRows(t, tableID, lastUpdatedIndexID, 2, store)

	reaper.Stop()
}

func TestMultipleTables(t *testing.T) {

	store := fake.NewFakeCluster(0, 10)
	reaper := NewReaper(store, 10000, shardID)
	reaper.startNoSchedule()

	colTypes := []common.ColumnType{common.BigIntColumnType, common.BigIntColumnType}

	tableID1 := uint64(23)
	lastUpdatedIndexID1 := uint64(24)
	retentionDuration1 := 500 * time.Millisecond
	tabInfo1 := common.NewTableInfo(tableID1, "test", "test_table1", []int{0}, []string{"col0", "last_updated"},
		colTypes, retentionDuration1, lastUpdatedIndexID1)

	tableID2 := uint64(25)
	lastUpdatedIndexID2 := uint64(26)
	retentionDuration2 := 2 * time.Second
	tabInfo2 := common.NewTableInfo(tableID2, "test", "test_table2", []int{0}, []string{"col0", "last_updated"},
		colTypes, retentionDuration2, lastUpdatedIndexID2)

	tableID3 := uint64(27)
	lastUpdatedIndexID3 := uint64(28)
	retentionDuration3 := 5 * time.Second
	tabInfo3 := common.NewTableInfo(tableID3, "test", "test_table3", []int{0}, []string{"col0", "last_updated"},
		colTypes, retentionDuration3, lastUpdatedIndexID3)

	reaper.addTableNoSchedule(tabInfo1)
	te1 := setupTableExecutor(store, tabInfo1, retentionDuration1, lastUpdatedIndexID1)

	reaper.addTableNoSchedule(tabInfo2)
	te2 := setupTableExecutor(store, tabInfo2, retentionDuration2, lastUpdatedIndexID2)

	reaper.addTableNoSchedule(tabInfo3)
	te3 := setupTableExecutor(store, tabInfo3, retentionDuration3, lastUpdatedIndexID3)

	now := time.Now()
	numRows := 5

	generateRows(t, numRows, 0, now.UnixMilli(), 0, te1, colTypes, store)
	generateRows(t, numRows, 0, now.UnixMilli(), 0, te2, colTypes, store)
	generateRows(t, numRows, 0, now.UnixMilli(), 0, te3, colTypes, store)

	dur, err := reaper.run(false)
	require.NoError(t, err)
	require.LessOrEqual(t, dur.Milliseconds(), int64(500))
	require.Greater(t, dur.Milliseconds(), int64(490)) // Margin of error
	checkNumRows(t, tableID1, lastUpdatedIndexID1, 5, store)
	checkNumRows(t, tableID2, lastUpdatedIndexID2, 5, store)
	checkNumRows(t, tableID3, lastUpdatedIndexID3, 5, store)

	time.Sleep(500 * time.Millisecond)
	dur, err = reaper.run(false)
	require.NoError(t, err)
	require.LessOrEqual(t, dur.Milliseconds(), int64(500))
	// Should be 500 again as it's possible that more rows could be added immediately for table1 so we need to check in 500ms
	require.Greater(t, dur.Milliseconds(), int64(490)) // Margin of error
	checkNumRows(t, tableID1, lastUpdatedIndexID1, 0, store)
	checkNumRows(t, tableID2, lastUpdatedIndexID2, 5, store)
	checkNumRows(t, tableID3, lastUpdatedIndexID3, 5, store)

	reaper.RemoveTable(tabInfo1)
	dur, err = reaper.run(false)
	require.NoError(t, err)
	require.LessOrEqual(t, dur.Milliseconds(), int64(1500))
	// table1 has been removed so next soonest is table 2 hence 1500 - but nothing should be deleted yet
	require.Greater(t, dur.Milliseconds(), int64(1490)) // Margin of error
	checkNumRows(t, tableID1, lastUpdatedIndexID1, 0, store)
	checkNumRows(t, tableID2, lastUpdatedIndexID2, 5, store)
	checkNumRows(t, tableID3, lastUpdatedIndexID3, 5, store)

	time.Sleep(1500 * time.Millisecond)
	dur, err = reaper.run(false)
	require.NoError(t, err)
	// Dur should be around 2000 - retention time of table 2 since more rows could arrive
	require.LessOrEqual(t, dur.Milliseconds(), int64(2000))
	require.Greater(t, dur.Milliseconds(), int64(1900)) // Margin of error

	reaper.RemoveTable(tabInfo2)
	dur, err = reaper.run(false)
	require.NoError(t, err)
	require.LessOrEqual(t, dur.Milliseconds(), int64(3000))
	require.Greater(t, dur.Milliseconds(), int64(2900)) // Margin of error
	checkNumRows(t, tableID1, lastUpdatedIndexID1, 0, store)
	checkNumRows(t, tableID2, lastUpdatedIndexID2, 0, store)
	checkNumRows(t, tableID3, lastUpdatedIndexID3, 5, store)

	time.Sleep(3000 * time.Millisecond)
	dur, err = reaper.run(false)
	require.NoError(t, err)
	require.LessOrEqual(t, dur.Milliseconds(), int64(5000))
	require.Greater(t, dur.Milliseconds(), int64(4900)) // Margin of error
	checkNumRows(t, tableID1, lastUpdatedIndexID1, 0, store)
	checkNumRows(t, tableID2, lastUpdatedIndexID2, 0, store)
	checkNumRows(t, tableID3, lastUpdatedIndexID3, 0, store)

	reaper.RemoveTable(tabInfo3)
	dur, err = reaper.run(false)
	require.NoError(t, err)
	require.Equal(t, int64(-1), int64(dur)) // Represents don't reschedule

	reaper.Stop()
}

func TestMultipleTablesNoRows(t *testing.T) {

	store := fake.NewFakeCluster(0, 10)
	reaper := NewReaper(store, 10000, shardID)
	reaper.startNoSchedule()

	colTypes := []common.ColumnType{common.BigIntColumnType, common.BigIntColumnType}

	tableID1 := uint64(23)
	lastUpdatedIndexID1 := uint64(24)
	retentionDuration1 := 500 * time.Millisecond
	tabInfo1 := common.NewTableInfo(tableID1, "test", "test_table1", []int{0}, []string{"col0", "last_updated"},
		colTypes, retentionDuration1, lastUpdatedIndexID1)

	tableID2 := uint64(25)
	lastUpdatedIndexID2 := uint64(26)
	retentionDuration2 := 2 * time.Second
	tabInfo2 := common.NewTableInfo(tableID2, "test", "test_table2", []int{0}, []string{"col0", "last_updated"},
		colTypes, retentionDuration2, lastUpdatedIndexID2)

	tableID3 := uint64(27)
	lastUpdatedIndexID3 := uint64(28)
	retentionDuration3 := 5 * time.Second
	tabInfo3 := common.NewTableInfo(tableID3, "test", "test_table3", []int{0}, []string{"col0", "last_updated"},
		colTypes, retentionDuration3, lastUpdatedIndexID3)

	reaper.addTableNoSchedule(tabInfo1)
	reaper.addTableNoSchedule(tabInfo2)
	reaper.addTableNoSchedule(tabInfo3)

	dur, err := reaper.run(false)
	require.NoError(t, err)
	require.LessOrEqual(t, dur.Milliseconds(), int64(500))
	require.Greater(t, dur.Milliseconds(), int64(490))

	reaper.RemoveTable(tabInfo1)
	dur, err = reaper.run(false)
	require.NoError(t, err)
	require.LessOrEqual(t, dur.Milliseconds(), int64(2000))
	require.Greater(t, dur.Milliseconds(), int64(1900))

	reaper.RemoveTable(tabInfo2)
	dur, err = reaper.run(false)
	require.NoError(t, err)
	require.LessOrEqual(t, dur.Milliseconds(), int64(5000))
	require.Greater(t, dur.Milliseconds(), int64(4900))

	reaper.RemoveTable(tabInfo3)
	dur, err = reaper.run(false)
	require.NoError(t, err)
	require.Equal(t, int64(-1), int64(dur))

	reaper.Stop()
}

func TestBatchSize(t *testing.T) {

	store := fake.NewFakeCluster(0, 10)
	reaper := NewReaper(store, 3, shardID)
	reaper.startNoSchedule()

	colTypes := []common.ColumnType{common.BigIntColumnType, common.BigIntColumnType}

	retentionDuration1 := 50 * time.Millisecond
	tableID1 := uint64(23)
	lastUpdatedIndexID1 := uint64(24)
	tabInfo1 := common.NewTableInfo(tableID1, "test", "test_table1", []int{0}, []string{"col0", "last_updated"},
		colTypes, retentionDuration1, lastUpdatedIndexID1)

	retentionDuration2 := 100 * time.Millisecond
	tableID2 := uint64(25)
	lastUpdatedIndexID2 := uint64(26)
	tabInfo2 := common.NewTableInfo(tableID2, "test", "test_table2", []int{0}, []string{"col0", "last_updated"},
		colTypes, retentionDuration2, lastUpdatedIndexID2)

	retentionDuration3 := 150 * time.Millisecond
	tableID3 := uint64(27)
	lastUpdatedIndexID3 := uint64(28)
	tabInfo3 := common.NewTableInfo(tableID3, "test", "test_table3", []int{0}, []string{"col0", "last_updated"},
		colTypes, retentionDuration3, lastUpdatedIndexID3)

	reaper.addTableNoSchedule(tabInfo1)
	te1 := setupTableExecutor(store, tabInfo1, retentionDuration1, lastUpdatedIndexID1)

	reaper.addTableNoSchedule(tabInfo2)
	te2 := setupTableExecutor(store, tabInfo2, retentionDuration2, lastUpdatedIndexID2)

	reaper.addTableNoSchedule(tabInfo3)
	te3 := setupTableExecutor(store, tabInfo3, retentionDuration3, lastUpdatedIndexID3)

	now := time.Now()
	numRows := 5

	generateRows(t, numRows, 0, now.UnixMilli(), 0, te1, colTypes, store)
	generateRows(t, numRows, 0, now.UnixMilli(), 0, te2, colTypes, store)
	generateRows(t, numRows, 0, now.UnixMilli(), 0, te3, colTypes, store)

	time.Sleep(retentionDuration3)
	// All the rows should be ready to reap but the batch size is only 3
	dur, err := reaper.run(false)
	require.NoError(t, err)
	require.Equal(t, time.Duration(0), dur)
	totRows := len(getTableRows(t, tableID1, store)) + len(getTableRows(t, tableID2, store)) + len(getTableRows(t, tableID3, store)) +
		len(getIndexRows(t, lastUpdatedIndexID1, store)) + len(getIndexRows(t, lastUpdatedIndexID2, store)) + len(getIndexRows(t, lastUpdatedIndexID3, store))
	require.Equal(t, 24, totRows)

	_, err = reaper.run(false)
	require.NoError(t, err)
	require.Equal(t, time.Duration(0), dur)
	totRows = len(getTableRows(t, tableID1, store)) + len(getTableRows(t, tableID2, store)) + len(getTableRows(t, tableID3, store)) +
		len(getIndexRows(t, lastUpdatedIndexID1, store)) + len(getIndexRows(t, lastUpdatedIndexID2, store)) + len(getIndexRows(t, lastUpdatedIndexID3, store))
	require.Equal(t, 18, totRows)

	_, err = reaper.run(false)
	require.NoError(t, err)
	totRows = len(getTableRows(t, tableID1, store)) + len(getTableRows(t, tableID2, store)) + len(getTableRows(t, tableID3, store)) +
		len(getIndexRows(t, lastUpdatedIndexID1, store)) + len(getIndexRows(t, lastUpdatedIndexID2, store)) + len(getIndexRows(t, lastUpdatedIndexID3, store))
	require.Equal(t, 12, totRows)

	_, err = reaper.run(false)
	require.NoError(t, err)
	totRows = len(getTableRows(t, tableID1, store)) + len(getTableRows(t, tableID2, store)) + len(getTableRows(t, tableID3, store)) +
		len(getIndexRows(t, lastUpdatedIndexID1, store)) + len(getIndexRows(t, lastUpdatedIndexID2, store)) + len(getIndexRows(t, lastUpdatedIndexID3, store))
	require.Equal(t, 6, totRows)

	dur, err = reaper.run(false)
	require.NoError(t, err)
	// It's zero because there *might* be more rows as we hit batch size exactly as we processed all rows
	require.Equal(t, int64(0), int64(dur))
	totRows = len(getTableRows(t, tableID1, store)) + len(getTableRows(t, tableID2, store)) + len(getTableRows(t, tableID3, store)) +
		len(getIndexRows(t, lastUpdatedIndexID1, store)) + len(getIndexRows(t, lastUpdatedIndexID2, store)) + len(getIndexRows(t, lastUpdatedIndexID3, store))
	require.Equal(t, 0, totRows)

	dur, err = reaper.run(false)
	require.NoError(t, err)
	// No more rows! The time returned will be approx the lowest retention time
	require.LessOrEqual(t, dur.Milliseconds(), retentionDuration1.Milliseconds())
	require.Greater(t, dur.Milliseconds(), time.Duration(float64(retentionDuration1.Milliseconds())*0.9))

	reaper.Stop()
}

func checkNumRows(t *testing.T, tableID uint64, indexID uint64, expectedRows int, store cluster.Cluster) {
	t.Helper()
	tableRows := getTableRows(t, tableID, store)
	require.Equal(t, expectedRows, len(tableRows))
	indexRows := getIndexRows(t, indexID, store)
	require.Equal(t, expectedRows, len(indexRows))
}

func getTableRows(t *testing.T, tableID uint64, store cluster.Cluster) []cluster.KVPair {
	t.Helper()
	tableScanPrefixStart := table.EncodeTableKeyPrefix(tableID, shardID, 16)
	tableScanPrefixEnd := common.IncrementBytesBigEndian(tableScanPrefixStart)
	tableRows, err := store.LocalScan(tableScanPrefixStart, tableScanPrefixEnd, -1)
	require.NoError(t, err)
	return tableRows
}

func getIndexRows(t *testing.T, indexID uint64, store cluster.Cluster) []cluster.KVPair {
	t.Helper()
	indexScanPrefixStart := table.EncodeTableKeyPrefix(indexID, shardID, 16)
	indexScanPrefixEnd := common.IncrementBytesBigEndian(indexScanPrefixStart)
	indexRows, err := store.LocalScan(indexScanPrefixStart, indexScanPrefixEnd, -1)
	require.NoError(t, err)
	return indexRows
}

func generateRows(t *testing.T, numRows int, startID int64, timeStart int64, timeInc int64, te *exec.TableExecutor,
	colTypes []common.ColumnType, store cluster.Cluster) {
	t.Helper()
	rf := common.NewRowsFactory(colTypes)
	rows := rf.NewRows(numRows)
	lastUpdate := timeStart
	for i := 0; i < numRows; i++ {
		rows.AppendInt64ToColumn(0, startID+int64(i))
		rows.AppendInt64ToColumn(1, lastUpdate)
		lastUpdate += timeInc
	}
	wb := cluster.NewWriteBatch(shardID)

	execCtx := exec.NewExecutionContext(wb, &clusterGetter{store: store}, -1)
	rb := exec.NewCurrentRowsBatch(rows)
	err := te.HandleRows(rb, execCtx)
	require.NoError(t, err)
	err = store.WriteBatchLocally(wb)
	require.NoError(t, err)
}

type clusterGetter struct {
	store cluster.Cluster
}

func (c *clusterGetter) Get(key []byte) ([]byte, error) {
	return c.store.LocalGet(key)
}

func setupTableExecutor(cluster cluster.Cluster, tabInfo *common.TableInfo, retentionDuration time.Duration, lastUpdatedIndexID uint64) *exec.TableExecutor {
	lastUpdateIndexName := "last_update_index"
	indexInfo := &common.IndexInfo{
		SchemaName: "test",
		ID:         lastUpdatedIndexID,
		TableName:  "test_table",
		Name:       lastUpdateIndexName,
		IndexCols:  []int{1},
	}
	indexExecutor := exec.NewIndexExecutor(tabInfo, indexInfo, cluster)
	te := exec.NewTableExecutor(tabInfo, cluster, false, retentionDuration)
	te.AddConsumingNode(lastUpdateIndexName, indexExecutor)
	return te
}
