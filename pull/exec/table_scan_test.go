package exec

import (
	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/table"
	"github.com/stretchr/testify/require"
	"testing"
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
	ts, clust := setupTableScan(t, inpRows)
	defer stopCluster(t, clust)

	exp := toRows(t, expectedRows, colTypes)

	provided, err := ts.GetRows(-1)
	require.NoError(t, err)
	require.NotNil(t, provided)
	common.AllRowsEqual(t, exp, provided, colTypes)

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

	ts, clust := setupTableScan(t, inpRows)
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

	ts, clust := setupTableScan(t, inpRows)
	defer stopCluster(t, clust)

	expectedRows1 := [][]interface{}{
		{1, "wincanton", 25.5, "132.45"},
		{2, "london", 35.1, "9.32"},
	}
	exp1 := toRows(t, expectedRows1, colTypes)
	provided, err := ts.GetRows(2)
	require.NoError(t, err)
	require.NotNil(t, provided)
	common.AllRowsEqual(t, exp1, provided, colTypes)

	expectedRows2 := [][]interface{}{
		{3, "los angeles", 20.6, "11.75"},
		{4, "sydney", 45.2, "4.99"},
	}
	exp2 := toRows(t, expectedRows2, colTypes)
	provided, err = ts.GetRows(2)
	require.NoError(t, err)
	require.NotNil(t, provided)
	common.AllRowsEqual(t, exp2, provided, colTypes)

	expectedRows3 := [][]interface{}{
		{5, "tokyo", 28.9, "999.99"},
	}
	exp3 := toRows(t, expectedRows3, colTypes)
	provided, err = ts.GetRows(2)
	require.NoError(t, err)
	require.NotNil(t, provided)
	common.AllRowsEqual(t, exp3, provided, colTypes)

	provided, err = ts.GetRows(2)
	require.NoError(t, err)
	require.NotNil(t, provided)
	require.Equal(t, 0, provided.RowCount())
}

func stopCluster(t *testing.T, clust cluster.Cluster) {
	t.Helper()
	err := clust.Stop()
	require.NoError(t, err)
}

func setupTableScan(t *testing.T, inputRows [][]interface{}) (PullExecutor, cluster.Cluster) {
	t.Helper()

	clust := cluster.NewFakeCluster(0, 10)
	clust.RegisterShardListenerFactory(&cluster.DummyShardListenerFactory{})
	clust.SetRemoteQueryExecutionCallback(&cluster.DummyRemoteQueryExecutionCallback{})
	err := clust.Start()
	require.NoError(t, err)

	shardID := clust.GetAllShardIDs()[0]
	tableID, err := clust.GenerateTableID()
	require.NoError(t, err)

	tableInfo := common.TableInfo{
		ID:             tableID,
		TableName:      "test_table",
		PrimaryKeyCols: []int{0},
		ColumnNames:    colNames,
		ColumnTypes:    colTypes,
		IndexInfos:     nil,
	}

	ts := NewPullTableScan(&tableInfo, clust, shardID)
	require.NoError(t, err)

	inpRows := toRows(t, inputRows, colTypes)
	batch := cluster.NewWriteBatch(shardID, false)
	for i := 0; i < inpRows.RowCount(); i++ {
		row := inpRows.GetRow(i)
		err = table.Upsert(&tableInfo, &row, batch)
		require.NoError(t, err)
	}
	err = clust.WriteBatch(batch)
	require.NoError(t, err)

	return ts, clust
}
