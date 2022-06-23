package exec

import (
	"github.com/squareup/pranadb/cluster/fake"
	"testing"

	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/common/commontest"
	"github.com/squareup/pranadb/table"
	"github.com/stretchr/testify/require"
)

func TestTableScanAllRows(t *testing.T) {
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
	ts, clust := setupTableScan(t, inpRows, []*ScanRange{nil}, colTypes, nil)
	defer stopCluster(t, clust)

	exp := toRows(t, expectedRows, colTypes)

	provided, err := ts.GetRows(1000)
	require.NoError(t, err)
	require.NotNil(t, provided)
	commontest.AllRowsEqual(t, exp, provided, colTypes)

	// Call again
	provided, err = ts.GetRows(1000)
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

	ts, clust := setupTableScan(t, inpRows, nil, colTypes, nil)
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

	ts, clust := setupTableScan(t, inpRows, []*ScanRange{nil}, colTypes, nil)
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
		LowVals:  []interface{}{int64(3)},
		HighVals: []interface{}{int64(4)},
		LowExcl:  false,
		HighExcl: false,
	}
	expectedRows := [][]interface{}{
		{3, "los angeles", 20.6, "11.75"},
		{4, "sydney", 45.2, "4.99"},
	}
	testTableScanWithRange(t, []*ScanRange{scanRange}, expectedRows)
}

func TestTableScanWithRangeIncLowExclHigh(t *testing.T) {
	scanRange := &ScanRange{
		LowVals:  []interface{}{int64(3)},
		HighVals: []interface{}{int64(4)},
		LowExcl:  false,
		HighExcl: true,
	}
	expectedRows := [][]interface{}{
		{3, "los angeles", 20.6, "11.75"},
	}
	testTableScanWithRange(t, []*ScanRange{scanRange}, expectedRows)
}

func TestTableScanWithRangeExclLowIncHigh(t *testing.T) {
	scanRange := &ScanRange{
		LowVals:  []interface{}{int64(3)},
		HighVals: []interface{}{int64(5)},
		LowExcl:  true,
		HighExcl: false,
	}
	expectedRows := [][]interface{}{
		{4, "sydney", 45.2, "4.99"},
		{5, "tokyo", 28.9, "999.99"},
	}
	testTableScanWithRange(t, []*ScanRange{scanRange}, expectedRows)
}

func TestTableScanWithRangeExclLowExclHigh(t *testing.T) {
	scanRange := &ScanRange{
		LowVals:  []interface{}{int64(1)},
		HighVals: []interface{}{int64(5)},
		LowExcl:  true,
		HighExcl: true,
	}
	expectedRows := [][]interface{}{
		{2, "london", 35.1, "9.32"},
		{3, "los angeles", 20.6, "11.75"},
		{4, "sydney", 45.2, "4.99"},
	}
	testTableScanWithRange(t, []*ScanRange{scanRange}, expectedRows)
}

func TestTableScanWithMultipleRanges(t *testing.T) {
	scanRange1 := &ScanRange{
		LowVals:  []interface{}{int64(1)},
		HighVals: []interface{}{int64(1)},
		LowExcl:  false,
		HighExcl: false,
	}
	scanRange2 := &ScanRange{
		LowVals:  []interface{}{int64(2)},
		HighVals: []interface{}{int64(3)},
		LowExcl:  false,
		HighExcl: true,
	}
	scanRange3 := &ScanRange{
		LowVals:  []interface{}{int64(4)},
		HighVals: []interface{}{int64(5)},
		LowExcl:  false,
		HighExcl: false,
	}
	expectedRows := [][]interface{}{
		{1, "wincanton", 25.5, "132.45"},
		{2, "london", 35.1, "9.32"},
		{4, "sydney", 45.2, "4.99"},
		{5, "tokyo", 28.9, "999.99"},
	}
	testTableScanWithRange(t, []*ScanRange{scanRange1, scanRange2, scanRange3}, expectedRows)
}

func testTableScanWithRange(t *testing.T, scanRanges []*ScanRange, expectedRows [][]interface{}) {
	t.Helper()
	inpRows := [][]interface{}{
		{1, "wincanton", 25.5, "132.45"},
		{2, "london", 35.1, "9.32"},
		{3, "los angeles", 20.6, "11.75"},
		{4, "sydney", 45.2, "4.99"},
		{5, "tokyo", 28.9, "999.99"},
	}

	ts, clust := setupTableScan(t, inpRows, scanRanges, colTypes, nil)
	defer stopCluster(t, clust)

	exp1 := toRows(t, expectedRows, colTypes)
	provided, err := ts.GetRows(1000)
	require.NoError(t, err)
	require.NotNil(t, provided)
	commontest.AllRowsEqual(t, exp1, provided, colTypes)
}

var naturalOrderingColTypes = []common.ColumnType{common.BigIntColumnType, common.DoubleColumnType,
	common.VarcharColumnType, common.NewDecimalColumnType(10, 3)}

/*
	expectedRows := [][]interface{}{
		{-300, 0.0, "str3", "-300.111"},
		{-200, 1.1, "str7", "0.000"},
		{-100, -1.1, "str1", "-100.111"},
		{0, -3.1, "str4", "300.111"},
		{100, -2.1, "str6", "-200.111"},
		{200, 3.1, "str5", "200.111"},
		{300, 2.1, "str2", "100.111"},
	}
*/

func TestTableScanNaturalOrderingBigIntCol(t *testing.T) {
	inpRows := [][]interface{}{
		{-200, 1.1, "str7", "0.000"},
		{300, 2.1, "str2", "100.111"},
		{0, -3.1, "str4", "300.111"},
		{-100, -1.1, "str1", "-100.111"},
		{200, 3.1, "str5", "200.111"},
		{-300, 0.0, "str3", "-300.111"},
		{100, -2.1, "str6", "-200.111"},
	}
	expectedRows := [][]interface{}{
		{-300, 0.0, "str3", "-300.111"},
		{-200, 1.1, "str7", "0.000"},
		{-100, -1.1, "str1", "-100.111"},
		{0, -3.1, "str4", "300.111"},
		{100, -2.1, "str6", "-200.111"},
		{200, 3.1, "str5", "200.111"},
		{300, 2.1, "str2", "100.111"},
	}
	testTableScanNaturalOrdering(t, inpRows, expectedRows, naturalOrderingColTypes, []int{0})
}

func TestTableScanNaturalOrderingDoubleCol(t *testing.T) {
	inpRows := [][]interface{}{
		{-200, 1.1, "str7", "0.000"},
		{300, 2.1, "str2", "100.111"},
		{0, -3.1, "str4", "300.111"},
		{-100, -1.1, "str1", "-100.111"},
		{200, 3.1, "str5", "200.111"},
		{-300, 0.0, "str3", "-300.111"},
		{100, -2.1, "str6", "-200.111"},
	}
	expectedRows := [][]interface{}{
		{0, -3.1, "str4", "300.111"},
		{100, -2.1, "str6", "-200.111"},
		{-100, -1.1, "str1", "-100.111"},
		{-300, 0.0, "str3", "-300.111"},
		{-200, 1.1, "str7", "0.000"},
		{300, 2.1, "str2", "100.111"},
		{200, 3.1, "str5", "200.111"},
	}
	testTableScanNaturalOrdering(t, inpRows, expectedRows, naturalOrderingColTypes, []int{1})
}

func TestTableScanNaturalOrderingStringCol(t *testing.T) {
	inpRows := [][]interface{}{
		{-200, 1.1, "zzz", "0.000"},
		{300, 2.1, "aaaa", "100.111"},
		{0, -3.1, "", "300.111"},
		{-100, -1.1, "z", "-100.111"},
		{200, 3.1, "zzzz", "200.111"},
		{-300, 0.0, "aaa", "-300.111"},
		{100, -2.1, "a", "-200.111"},
	}
	expectedRows := [][]interface{}{
		{0, -3.1, "", "300.111"},
		{100, -2.1, "a", "-200.111"},
		{-300, 0.0, "aaa", "-300.111"},
		{300, 2.1, "aaaa", "100.111"},
		{-100, -1.1, "z", "-100.111"},
		{-200, 1.1, "zzz", "0.000"},
		{200, 3.1, "zzzz", "200.111"},
	}
	testTableScanNaturalOrdering(t, inpRows, expectedRows, naturalOrderingColTypes, []int{2})
}

func TestTableScanNaturalOrderingDecimalCol(t *testing.T) {
	inpRows := [][]interface{}{
		{-200, 1.1, "zzz", "0.000"},
		{300, 2.1, "aaaa", "100.111"},
		{0, -3.1, "", "300.111"},
		{-100, -1.1, "z", "-100.111"},
		{200, 3.1, "zzzz", "200.111"},
		{-300, 0.0, "aaa", "-300.111"},
		{100, -2.1, "a", "-200.111"},
	}
	expectedRows := [][]interface{}{
		{-300, 0.0, "aaa", "-300.111"},
		{100, -2.1, "a", "-200.111"},
		{-100, -1.1, "z", "-100.111"},
		{-200, 1.1, "zzz", "0.000"},
		{300, 2.1, "aaaa", "100.111"},
		{200, 3.1, "zzzz", "200.111"},
		{0, -3.1, "", "300.111"},
	}
	testTableScanNaturalOrdering(t, inpRows, expectedRows, naturalOrderingColTypes, []int{3})
}

func TestTableScanNaturalOrderingCompositeKey(t *testing.T) {
	inpRows := [][]interface{}{
		{200, 2.2, "zzz", "0.000"},
		{-300, -3.1, "z", "-200.111"},
		{0, 0.2, "zzz", "0.000"},
		{200, 2.1, "z", "-100.111"},
		{-300, -3.0, "a", "-300.111"},
		{100, 1.1, "z", "-100.111"},
		{-200, -2.8, "zzz", "0.000"},
		{300, 3.2, "zzz", "0.000"},
		{-100, -1.7, "z", "-100.111"},
		{-100, -1.6, "zzz", "0.000"},
		{0, 0.1, "z", "-100.111"},
		{100, 1.2, "zzz", "0.000"},
		{300, 3.1, "z", "-100.111"},
		{-200, -2.9, "z", "-100.111"},
	}
	expectedRows := [][]interface{}{
		{-300, -3.1, "z", "-200.111"},
		{-300, -3.0, "a", "-300.111"},
		{-200, -2.9, "z", "-100.111"},
		{-200, -2.8, "zzz", "0.000"},
		{-100, -1.7, "z", "-100.111"},
		{-100, -1.6, "zzz", "0.000"},
		{0, 0.1, "z", "-100.111"},
		{0, 0.2, "zzz", "0.000"},
		{100, 1.1, "z", "-100.111"},
		{100, 1.2, "zzz", "0.000"},
		{200, 2.1, "z", "-100.111"},
		{200, 2.2, "zzz", "0.000"},
		{300, 3.1, "z", "-100.111"},
		{300, 3.2, "zzz", "0.000"},
	}
	testTableScanNaturalOrdering(t, inpRows, expectedRows, naturalOrderingColTypes, []int{0, 1})
}

func testTableScanNaturalOrdering(t *testing.T, inpRows [][]interface{}, expectedRows [][]interface{}, colTypes []common.ColumnType, pkCols []int) {
	t.Helper()
	ts, clust := setupTableScan(t, inpRows, []*ScanRange{nil}, colTypes, pkCols)
	defer stopCluster(t, clust)

	exp := toRows(t, expectedRows, colTypes)

	provided, err := ts.GetRows(1000)
	require.NoError(t, err)
	require.NotNil(t, provided)
	commontest.AllRowsEqual(t, exp, provided, colTypes)
}

func stopCluster(t *testing.T, clust cluster.Cluster) {
	t.Helper()
	err := clust.Stop()
	require.NoError(t, err)
}

func setupTableScan(t *testing.T, inputRows [][]interface{}, scanRanges []*ScanRange, colTypes []common.ColumnType, pkCols []int) (PullExecutor, cluster.Cluster) {
	t.Helper()

	clust := fake.NewFakeCluster(0, 10)
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

	ts, err := NewPullTableScan(&tableInfo, nil, clust, shardID, scanRanges)
	require.NoError(t, err)

	return ts, clust
}

func insertRowsIntoTable(t *testing.T, shardID uint64, tableInfo *common.TableInfo, inpRows *common.Rows, clust cluster.Cluster) {
	t.Helper()
	batch := cluster.NewWriteBatch(shardID)
	for i := 0; i < inpRows.RowCount(); i++ {
		row := inpRows.GetRow(i)
		err := table.Upsert(tableInfo, &row, batch)
		require.NoError(t, err)
	}
	err := clust.WriteBatch(batch)
	require.NoError(t, err)
}
