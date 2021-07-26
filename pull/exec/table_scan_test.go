package exec

import (
	"log"
	"testing"

	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/common/commontest"
	"github.com/squareup/pranadb/table"
	"github.com/stretchr/testify/require"
)

func TestTableScanNoLimit(t *testing.T) {
	inpRows := [][]interface{}{
		{1, "wincanton", 25.5, "132.45"},
		{2, "london", 35.1, "9.32"},
		{3, "los angeles", 20.6, "11.75"},
	}
	expectedRows := [][]interface{}{
		{1, "wincanton", 25.5, "132.45"},
		{2, "london", 35.1, "9.32"},
		{3, "los angeles", 20.6, "11.75"},
	}
	ts, clust := setupTableScan(t, inpRows, nil, nil)
	defer stopCluster(t, clust)

	exp := toRows(t, expectedRows, colTypes)

	provided, err := ts.GetRows(-1)
	require.NoError(t, err)
	require.NotNil(t, provided)
	commontest.AllRowsEqual(t, exp, provided, colTypes)

	// Call again
	provided, err = ts.GetRows(-1)
	require.NoError(t, err)
	require.NotNil(t, provided)
	require.Equal(t, 0, provided.RowCount())
}

func TestTableScanWithLimit0(t *testing.T) {
	inpRows := [][]interface{}{
		{1, "wincanton", 25.5, "132.45"},
		{2, "london", 35.1, "9.32"},
		{3, "los angeles", 20.6, "11.75"},
	}

	ts, clust := setupTableScan(t, inpRows, nil, nil)
	defer stopCluster(t, clust)

	_, err := ts.GetRows(0)
	require.NotNil(t, err)
}

func TestTableScanWithLimit2(t *testing.T) {
	inpRows := [][]interface{}{
		{1, "wincanton", 25.5, "132.45"},
		{2, "london", 35.1, "9.32"},
		{3, "los angeles", 20.6, "11.75"},
		{4, "sydney", 45.2, "4.99"},
		{5, "tokyo", 28.9, "999.99"},
	}

	ts, clust := setupTableScan(t, inpRows, nil, nil)
	defer stopCluster(t, clust)

	expectedRows1 := [][]interface{}{
		{1, "wincanton", 25.5, "132.45"},
		{2, "london", 35.1, "9.32"},
	}
	exp1 := toRows(t, expectedRows1, colTypes)
	provided, err := ts.GetRows(2)
	require.NoError(t, err)
	require.NotNil(t, provided)
	commontest.AllRowsEqual(t, exp1, provided, colTypes)

	expectedRows2 := [][]interface{}{
		{3, "los angeles", 20.6, "11.75"},
		{4, "sydney", 45.2, "4.99"},
	}
	exp2 := toRows(t, expectedRows2, colTypes)
	provided, err = ts.GetRows(2)
	require.NoError(t, err)
	require.NotNil(t, provided)
	commontest.AllRowsEqual(t, exp2, provided, colTypes)

	expectedRows3 := [][]interface{}{
		{5, "tokyo", 28.9, "999.99"},
	}
	exp3 := toRows(t, expectedRows3, colTypes)
	provided, err = ts.GetRows(2)
	require.NoError(t, err)
	require.NotNil(t, provided)
	commontest.AllRowsEqual(t, exp3, provided, colTypes)

	provided, err = ts.GetRows(2)
	require.NoError(t, err)
	require.NotNil(t, provided)
	require.Equal(t, 0, provided.RowCount())
}

func TestTableScanWithRangeIncLowIncHigh(t *testing.T) {
	scanRange := &ScanRange{
		LowVal:   3,
		HighVal:  4,
		LowExcl:  false,
		HighExcl: false,
	}
	expectedRows := [][]interface{}{
		{3, "los angeles", 20.6, "11.75"},
		{4, "sydney", 45.2, "4.99"},
	}
	testTableScanWithRange(t, scanRange, expectedRows)
}

func TestTableScanWithRangeIncLowExclHigh(t *testing.T) {
	scanRange := &ScanRange{
		LowVal:   3,
		HighVal:  4,
		LowExcl:  false,
		HighExcl: true,
	}
	expectedRows := [][]interface{}{
		{3, "los angeles", 20.6, "11.75"},
	}
	testTableScanWithRange(t, scanRange, expectedRows)
}

func TestTableScanWithRangeExclLowIncHigh(t *testing.T) {
	scanRange := &ScanRange{
		LowVal:   3,
		HighVal:  5,
		LowExcl:  true,
		HighExcl: false,
	}
	expectedRows := [][]interface{}{
		{4, "sydney", 45.2, "4.99"},
		{5, "tokyo", 28.9, "999.99"},
	}
	testTableScanWithRange(t, scanRange, expectedRows)
}

func TestTableScanWithRangeExclLowExclHigh(t *testing.T) {
	scanRange := &ScanRange{
		LowVal:   1,
		HighVal:  5,
		LowExcl:  true,
		HighExcl: true,
	}
	expectedRows := [][]interface{}{
		{2, "london", 35.1, "9.32"},
		{3, "los angeles", 20.6, "11.75"},
		{4, "sydney", 45.2, "4.99"},
	}
	testTableScanWithRange(t, scanRange, expectedRows)
}

func testTableScanWithRange(t *testing.T, scanRange *ScanRange, expectedRows [][]interface{}) {
	t.Helper()
	inpRows := [][]interface{}{
		{1, "wincanton", 25.5, "132.45"},
		{2, "london", 35.1, "9.32"},
		{3, "los angeles", 20.6, "11.75"},
		{4, "sydney", 45.2, "4.99"},
		{5, "tokyo", 28.9, "999.99"},
	}

	ts, clust := setupTableScan(t, inpRows, scanRange, nil)
	defer stopCluster(t, clust)

	exp1 := toRows(t, expectedRows, colTypes)
	provided, err := ts.GetRows(-1)
	require.NoError(t, err)
	require.NotNil(t, provided)
	commontest.AllRowsEqual(t, exp1, provided, colTypes)
}

func TestTableScanNaturalOrderingIntCol(t *testing.T) {
	inpRows := [][]interface{}{
		{3, "los angeles", 20.6, "11.75"},
		{1, "wincanton", 25.5, "132.45"},
		{4, "sydney", 45.2, "4.99"},
		{5, "tokyo", 28.9, "999.99"},
		{2, "london", 35.1, "9.32"},
	}
	expectedRows := [][]interface{}{
		{1, "wincanton", 25.5, "132.45"},
		{2, "london", 35.1, "9.32"},
		{3, "los angeles", 20.6, "11.75"},
		{4, "sydney", 45.2, "4.99"},
		{5, "tokyo", 28.9, "999.99"},
	}
	testTableScanNaturalOrdering(t, inpRows, expectedRows, []int{0})
}

// TODO natural ordering for some tests is currently broken - will be fixed in next PR
//func TestTableScanNaturalOrderingDoubleCol(t *testing.T) {
//	inpRows := [][]interface{}{
//		{1, "wincanton", 67.1, "132.45"},
//		{2, "london", 22.4, "9.32"},
//		{3, "los angeles", 44.2, "11.75"},
//		{4, "sydney", 0.2, "4.99"},
//		{5, "tokyo", -56.1, "999.99"},
//	}
//	expectedRows := [][]interface{}{
//		{5, "tokyo", -56.1, "999.99"},
//		{4, "sydney", 0.2, "4.99"},
//		{2, "london", 22.4, "9.32"},
//		{3, "los angeles", 44.2, "11.75"},
//		{1, "wincanton", 67.1, "132.45"},
//	}
//	testTableScanNaturalOrdering(t, inpRows, expectedRows, []int{2})
//}

func testTableScanNaturalOrdering(t *testing.T, inpRows [][]interface{}, expectedRows [][]interface{}, pkCols []int) {
	t.Helper()
	ts, clust := setupTableScan(t, inpRows, nil, pkCols)
	defer stopCluster(t, clust)

	exp := toRows(t, expectedRows, colTypes)

	provided, err := ts.GetRows(-1)
	require.NoError(t, err)
	require.NotNil(t, provided)
	for i := 0; i < provided.RowCount(); i++ {
		row := provided.GetRow(i)
		log.Printf("Got row: %s", row.String())
	}
	commontest.AllRowsEqual(t, exp, provided, colTypes)
}

func stopCluster(t *testing.T, clust cluster.Cluster) {
	t.Helper()
	err := clust.Stop()
	require.NoError(t, err)
}

func setupTableScan(t *testing.T, inputRows [][]interface{}, scanRange *ScanRange, pkCols []int) (PullExecutor, cluster.Cluster) {
	t.Helper()

	clust := cluster.NewFakeCluster(0, 10)
	clust.RegisterShardListenerFactory(&cluster.DummyShardListenerFactory{})
	clust.SetRemoteQueryExecutionCallback(&cluster.DummyRemoteQueryExecutionCallback{})
	err := clust.Start()
	require.NoError(t, err)

	shardID := clust.GetAllShardIDs()[0]

	tableID := common.UserTableIDBase + uint64(1)

	if pkCols == nil {
		pkCols = []int{0}
	}
	tableInfo := common.TableInfo{
		ID:             tableID,
		SchemaName:     "test",
		Name:           "test_table",
		PrimaryKeyCols: pkCols,
		ColumnNames:    colNames,
		ColumnTypes:    colTypes,
		IndexInfos:     nil,
	}

	inpRows := toRows(t, inputRows, colTypes)

	insertRowsIntoTable(t, shardID, &tableInfo, inpRows, clust)

	// Also insert the rows in another couple of tables one either side of the table id, to test boundary
	// conditions

	tableInfoBefore := tableInfo
	tableInfoBefore.ID = tableInfo.ID - 1

	insertRowsIntoTable(t, shardID, &tableInfoBefore, inpRows, clust)

	tableInfoAfter := tableInfo
	tableInfoAfter.ID = tableInfo.ID + 1

	insertRowsIntoTable(t, shardID, &tableInfoAfter, inpRows, clust)

	ts, err := NewPullTableScan(&tableInfo, clust, shardID, scanRange)
	require.NoError(t, err)

	return ts, clust
}

func insertRowsIntoTable(t *testing.T, shardID uint64, tableInfo *common.TableInfo, inpRows *common.Rows, clust cluster.Cluster) {
	t.Helper()
	batch := cluster.NewWriteBatch(shardID, false)
	for i := 0; i < inpRows.RowCount(); i++ {
		row := inpRows.GetRow(i)
		err := table.Upsert(tableInfo, &row, batch)
		require.NoError(t, err)
	}
	err := clust.WriteBatch(batch)
	require.NoError(t, err)
}
