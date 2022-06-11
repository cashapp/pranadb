package exec

import (
	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/cluster/fake"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/common/commontest"
	"github.com/squareup/pranadb/table"
	"github.com/stretchr/testify/require"
	"testing"
)

func setup() (inputRows [][]interface{}, tableColNames []string, tableColTypes []common.ColumnType) {
	// We jumble up the rows to make sure they are returned in natural index order
	inputRows = [][]interface{}{
		{3, 2, "ccc"},
		{5, nil, "ddd"},
		{2, 2, "bbb"},
		{1, 1, "aaa"},
		{4, nil, "ddd"},
	}
	tableColNames = []string{"pk", "col1", "col2"}
	tableColTypes = []common.ColumnType{common.BigIntColumnType, common.BigIntColumnType, common.VarcharColumnType}
	return
}

func TestReadIndexCovers(t *testing.T) {
	inpRows, tableColNames, tableColTypes := setup()

	pkCols := []int{0}
	indexCols := []int{1}
	tableColIndexes := []int{0, 1}
	expectedColTypes := []common.ColumnType{common.BigIntColumnType, common.BigIntColumnType}

	// Test with ranges of a single value
	expectedRows := [][]interface{}{{2, 2}, {3, 2}}
	ranges := createSimpleRanges(int64(2), int64(2), false, false)
	testReadIndex(t, indexCols, pkCols, ranges, tableColIndexes, inpRows, tableColNames, tableColTypes, expectedRows, expectedColTypes)

	ranges = createSimpleRanges(int64(1), int64(1), false, false)
	expectedRows = [][]interface{}{{1, 1}}
	testReadIndex(t, indexCols, pkCols, ranges, tableColIndexes, inpRows, tableColNames, tableColTypes, expectedRows, expectedColTypes)

	ranges = createSimpleRanges(int64(7), int64(7), false, false)
	expectedRows = [][]interface{}{}
	testReadIndex(t, indexCols, pkCols, ranges, tableColIndexes, inpRows, tableColNames, tableColTypes, expectedRows, expectedColTypes)

	ranges = createSimpleRanges(nil, nil, false, false)
	expectedRows = [][]interface{}{{4, nil}, {5, nil}}
	testReadIndex(t, indexCols, pkCols, ranges, tableColIndexes, inpRows, tableColNames, tableColTypes, expectedRows, expectedColTypes)

	// Range with no lower bound, but higher bound
	ranges = createSimpleRanges(nil, int64(2), false, true)
	expectedRows = [][]interface{}{{4, nil}, {5, nil}, {1, 1}}
	testReadIndex(t, indexCols, pkCols, ranges, tableColIndexes, inpRows, tableColNames, tableColTypes, expectedRows, expectedColTypes)

	// Simple range that excludes the first element
	ranges = createSimpleRanges(int64(1), int64(1), true, false)
	expectedRows = [][]interface{}{}
	testReadIndex(t, indexCols, pkCols, ranges, tableColIndexes, inpRows, tableColNames, tableColTypes, expectedRows, expectedColTypes)

	// Simple range that excludes the last element
	ranges = createSimpleRanges(int64(1), int64(1), false, true)
	expectedRows = [][]interface{}{}
	testReadIndex(t, indexCols, pkCols, ranges, tableColIndexes, inpRows, tableColNames, tableColTypes, expectedRows, expectedColTypes)

	// Simple range that excludes both elements
	ranges = createSimpleRanges(int64(1), int64(1), true, true)
	expectedRows = [][]interface{}{}
	testReadIndex(t, indexCols, pkCols, ranges, tableColIndexes, inpRows, tableColNames, tableColTypes, expectedRows, expectedColTypes)

	// Test with no range - full index scan
	expectedRows = [][]interface{}{
		{4, nil},
		{5, nil},
		{1, 1},
		{2, 2},
		{3, 2},
	}
	testReadIndex(t, indexCols, pkCols, nil, tableColIndexes, inpRows, tableColNames, tableColTypes, expectedRows, expectedColTypes)

	// Open ended range including first element
	ranges = createSimpleRanges(int64(2), nil, false, false)
	expectedRows = [][]interface{}{
		{2, 2},
		{3, 2},
	}
	testReadIndex(t, indexCols, pkCols, ranges, tableColIndexes, inpRows, tableColNames, tableColTypes, expectedRows, expectedColTypes)

	// Open ended range excluding first element
	ranges = createSimpleRanges(int64(1), nil, true, false)
	expectedRows = [][]interface{}{
		{2, 2},
		{3, 2},
	}
	testReadIndex(t, indexCols, pkCols, ranges, tableColIndexes, inpRows, tableColNames, tableColTypes, expectedRows, expectedColTypes)

	// Not returning PK
	tableColIndexes = []int{1}
	expectedColTypes = []common.ColumnType{common.BigIntColumnType}
	ranges = createSimpleRanges(int64(1), int64(1), false, false)
	expectedRows = [][]interface{}{
		{1},
	}
	testReadIndex(t, indexCols, pkCols, ranges, tableColIndexes, inpRows, tableColNames, tableColTypes, expectedRows, expectedColTypes)

	ranges = createSimpleRanges(int64(2), int64(2), false, false)
	expectedRows = [][]interface{}{
		{2}, {2},
	}
	testReadIndex(t, indexCols, pkCols, ranges, tableColIndexes, inpRows, tableColNames, tableColTypes, expectedRows, expectedColTypes)

	tableColIndexes = []int{1, 2}
	expectedColTypes = []common.ColumnType{common.BigIntColumnType, common.VarcharColumnType}
	expectedRows = [][]interface{}{
		{1, "aaa"},
	}
	ranges = createSimpleRanges(int64(1), int64(1), false, false)
	testReadIndex(t, indexCols, pkCols, ranges, tableColIndexes, inpRows, tableColNames, tableColTypes, expectedRows, expectedColTypes)

	expectedRows = [][]interface{}{
		{2, "bbb"},
		{2, "ccc"},
	}
	ranges = createSimpleRanges(int64(2), int64(2), false, false)
	testReadIndex(t, indexCols, pkCols, ranges, tableColIndexes, inpRows, tableColNames, tableColTypes, expectedRows, expectedColTypes)

	// Just the PK
	tableColIndexes = []int{0}
	expectedColTypes = []common.ColumnType{common.BigIntColumnType}
	expectedRows = [][]interface{}{{2}, {3}}
	ranges = createSimpleRanges(int64(2), int64(2), false, false)
	testReadIndex(t, indexCols, pkCols, ranges, tableColIndexes, inpRows, tableColNames, tableColTypes, expectedRows, expectedColTypes)
}

func TestReadIndexNotCovers(t *testing.T) {
	// We jumble up the rows to make sure they are returned in natural index order
	inpRows := [][]interface{}{
		{3, 2, "ccc"},
		{5, nil, "ddd"},
		{2, 2, "bbb"},
		{1, 1, "aaa"},
		{4, nil, "ddd"},
	}
	tableColNames := []string{"pk", "col1", "col2"}
	tableColTypes := []common.ColumnType{common.BigIntColumnType, common.BigIntColumnType, common.VarcharColumnType}
	pkCols := []int{0}
	indexCols := []int{1}

	tableColIndexes := []int{0, 1, 2}
	expectedColTypes := []common.ColumnType{common.BigIntColumnType, common.BigIntColumnType, common.VarcharColumnType}
	expectedRows := [][]interface{}{{2, 2, "bbb"}, {3, 2, "ccc"}}
	ranges := createSimpleRanges(int64(2), int64(2), false, false)
	testReadIndex(t, indexCols, pkCols, ranges, tableColIndexes, inpRows, tableColNames, tableColTypes, expectedRows, expectedColTypes)

	// Select just a row that's not in the index
	tableColIndexes = []int{2}
	expectedColTypes = []common.ColumnType{common.VarcharColumnType}
	expectedRows = [][]interface{}{{"bbb"}, {"ccc"}}
	ranges = createSimpleRanges(int64(2), int64(2), false, false)
	testReadIndex(t, indexCols, pkCols, ranges, tableColIndexes, inpRows, tableColNames, tableColTypes, expectedRows, expectedColTypes)
}

func TestCompositePK(t *testing.T) {
	inpRows := [][]interface{}{
		{2, 1, 2, "ccc"},
		{3, 1, nil, "ddd"},
		{1, 2, 2, "bbb"},
		{1, 1, 1, "aaa"},
		{2, 2, nil, "ddd"},
	}
	tableColNames := []string{"pk1", "pk2", "col1", "col2"}
	tableColTypes := []common.ColumnType{common.BigIntColumnType, common.BigIntColumnType, common.BigIntColumnType, common.VarcharColumnType}
	pkCols := []int{0, 1}
	indexCols := []int{2}

	// Covers
	tableColIndexes := []int{0, 1, 2}
	expectedColTypes := []common.ColumnType{common.BigIntColumnType, common.BigIntColumnType, common.BigIntColumnType}
	expectedRows := [][]interface{}{
		{1, 2, 2},
		{2, 1, 2},
	}
	ranges := createSimpleRanges(int64(2), int64(2), false, false)
	testReadIndex(t, indexCols, pkCols, ranges, tableColIndexes, inpRows, tableColNames, tableColTypes, expectedRows, expectedColTypes)

	// Not covers
	tableColIndexes = []int{0, 1, 2, 3}
	expectedColTypes = []common.ColumnType{common.BigIntColumnType, common.BigIntColumnType, common.BigIntColumnType, common.VarcharColumnType}
	expectedRows = [][]interface{}{
		{1, 2, 2, "bbb"},
		{2, 1, 2, "ccc"},
	}
	ranges = createSimpleRanges(int64(2), int64(2), false, false)
	testReadIndex(t, indexCols, pkCols, ranges, tableColIndexes, inpRows, tableColNames, tableColTypes, expectedRows, expectedColTypes)

}

func TestCompositeIndex(t *testing.T) {
	inpRows := [][]interface{}{
		{2, 1, 2, "ccc"},
		{3, 1, nil, "ddd"},
		{1, 2, 2, "bbb"},
		{1, 1, 1, "aaa"},
		{2, 2, nil, "ddd"},
	}
	tableColNames := []string{"pk1", "pk2", "col1", "col2"}
	tableColTypes := []common.ColumnType{common.BigIntColumnType, common.BigIntColumnType, common.BigIntColumnType, common.VarcharColumnType}
	pkCols := []int{0, 1}
	indexCols := []int{2, 3}

	// Covers
	// Fix first column
	tableColIndexes := []int{0, 1, 2, 3}
	expectedColTypes := []common.ColumnType{common.BigIntColumnType, common.BigIntColumnType, common.BigIntColumnType, common.VarcharColumnType}
	expectedRows := [][]interface{}{
		{1, 2, 2, "bbb"},
		{2, 1, 2, "ccc"},
	}
	ranges := createSimpleRanges(int64(2), int64(2), false, false)
	testReadIndex(t, indexCols, pkCols, ranges, tableColIndexes, inpRows, tableColNames, tableColTypes, expectedRows, expectedColTypes)

	// Fix both columns
	expectedRows = [][]interface{}{
		{1, 2, 2, "bbb"},
	}
	scanRange := &ScanRange{
		LowVals:  []interface{}{int64(2), "bbb"},
		HighVals: []interface{}{int64(2), "bbb"},
		LowExcl:  false,
		HighExcl: false,
	}
	ranges = []*ScanRange{scanRange}
	testReadIndex(t, indexCols, pkCols, ranges, tableColIndexes, inpRows, tableColNames, tableColTypes, expectedRows, expectedColTypes)

	// Fix first column, have open range in second column
	expectedRows = [][]interface{}{
		{1, 2, 2, "bbb"},
		{2, 1, 2, "ccc"},
	}
	scanRange = &ScanRange{
		LowVals:  []interface{}{int64(2), "bbb"},
		HighVals: []interface{}{int64(2), nil},
		LowExcl:  false,
		HighExcl: false,
	}
	ranges = []*ScanRange{scanRange}
	testReadIndex(t, indexCols, pkCols, ranges, tableColIndexes, inpRows, tableColNames, tableColTypes, expectedRows, expectedColTypes)

	// Fix first column exclusive, have open range in second column
	expectedRows = [][]interface{}{
		{2, 1, 2, "ccc"},
	}
	scanRange = &ScanRange{
		LowVals:  []interface{}{int64(2), "bbb"},
		HighVals: []interface{}{int64(2), nil},
		LowExcl:  true,
		HighExcl: false,
	}
	ranges = []*ScanRange{scanRange}
	testReadIndex(t, indexCols, pkCols, ranges, tableColIndexes, inpRows, tableColNames, tableColTypes, expectedRows, expectedColTypes)
}

func createSimpleRanges(lowVal interface{}, highVal interface{}, lowExcl bool, highExcl bool) []*ScanRange {
	scanRange := &ScanRange{
		LowVals:  []interface{}{lowVal},
		HighVals: []interface{}{highVal},
		LowExcl:  lowExcl,
		HighExcl: highExcl,
	}
	return []*ScanRange{scanRange}
}

func testReadIndex(t *testing.T, indexCols []int, pkCols []int, ranges []*ScanRange, tableColIndexes []int, inpRows [][]interface{},
	tableColNames []string, tableColTypes []common.ColumnType, expectedRows [][]interface{}, expectedColTypes []common.ColumnType) {
	t.Helper()
	testIndexReader(t, indexCols, pkCols, ranges, tableColIndexes, inpRows, tableColNames, tableColTypes, expectedRows, expectedColTypes)
}

func testIndexReader(t *testing.T, indexCols []int, pkCols []int, scanRanges []*ScanRange,
	tableColIndexes []int, inpRows [][]interface{}, tableColNames []string,
	tableColTypes []common.ColumnType, expectedRows [][]interface{}, expectedColTypes []common.ColumnType) {
	t.Helper()
	indexInfo := &common.IndexInfo{
		SchemaName: "test",
		ID:         1000000,
		TableName:  "test_table",
		Name:       "test_index",
		IndexCols:  indexCols,
	}
	ir, clust := setupIndexReader(t, inpRows, scanRanges, tableColIndexes, tableColNames, tableColTypes, pkCols, indexInfo)
	defer stopCluster(t, clust)
	exp := toRows(t, expectedRows, expectedColTypes)
	provided, err := ir.GetRows(100)
	require.NoError(t, err)
	require.NotNil(t, provided)
	commontest.AllRowsEqual(t, exp, provided, expectedColTypes)
}

func setupIndexReader(t *testing.T, inputRows [][]interface{}, scanRanges []*ScanRange, colIndexes []int, colNames []string,
	colTypes []common.ColumnType, pkCols []int, indexInfo *common.IndexInfo) (PullExecutor, cluster.Cluster) {
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
		IndexInfos: map[string]*common.IndexInfo{
			indexInfo.Name: indexInfo,
		},
	}

	inpRows := toRows(t, inputRows, colTypes)

	insertRowsIntoTableAndIndex(t, shardID, &tableInfo, indexInfo, inpRows, clust)

	// Also insert the rows in another couple of tables one either side of the table id, to test boundary
	// conditions

	tableInfoBefore := tableInfo
	tableInfoBefore.ID = tableInfo.ID - 1
	indexInfoBefore := *indexInfo
	indexInfoBefore.ID = indexInfo.ID - 1

	insertRowsIntoTableAndIndex(t, shardID, &tableInfoBefore, &indexInfoBefore, inpRows, clust)

	tableInfoAfter := tableInfo
	tableInfoAfter.ID = tableInfo.ID + 1
	indexInfoAfter := *indexInfo
	indexInfoAfter.ID = indexInfo.ID + 1

	insertRowsIntoTableAndIndex(t, shardID, &tableInfoAfter, &indexInfoAfter, inpRows, clust)

	is, err := NewPullIndexReader(&tableInfo, indexInfo, colIndexes, clust, shardID, scanRanges)
	require.NoError(t, err)

	return is, clust
}

func insertRowsIntoTableAndIndex(t *testing.T, shardID uint64, tableInfo *common.TableInfo, indexInfo *common.IndexInfo,
	inpRows *common.Rows, clust cluster.Cluster) {
	t.Helper()
	insertRowsIntoTable(t, shardID, tableInfo, inpRows, clust)
	batch := cluster.NewWriteBatch(shardID)
	for i := 0; i < inpRows.RowCount(); i++ {
		row := inpRows.GetRow(i)
		k, v, err := table.EncodeIndexKeyValue(tableInfo, indexInfo, shardID, &row)
		batch.AddPut(k, v)
		require.NoError(t, err)
	}
	err := clust.WriteBatch(batch)
	require.NoError(t, err)
}
