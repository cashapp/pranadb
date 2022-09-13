package exec

import (
	"fmt"
	"sync"
	"testing"

	"github.com/squareup/pranadb/interruptor"
	"github.com/squareup/pranadb/remoting"

	"github.com/squareup/pranadb/meta"

	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/common/commontest"
	"github.com/squareup/pranadb/sharder"
	"github.com/stretchr/testify/require"
)

func TestRemoteExecutorGetAll(t *testing.T) {
	numRows := 100
	rf := common.NewRowsFactory(colTypes)
	re, allRows, _ := setupRowExecutor(t, numRows, rf)

	provided, err := re.GetRows(numRows)
	require.NoError(t, err)
	require.NotNil(t, provided)
	require.Equal(t, numRows, provided.RowCount())

	arrRows := commontest.RowsToSlice(provided)
	commontest.SortRows(arrRows)
	arrExpectedRows := commontest.RowsToSlice(allRows)
	for i := 0; i < len(arrRows); i++ {
		commontest.RowsEqual(t, *arrExpectedRows[i], *arrRows[i], colTypes)
	}
}

func TestRemoteExecutorGetAllRequestMany(t *testing.T) {
	numRows := 100
	rf := common.NewRowsFactory(colTypes)
	re, allRows, _ := setupRowExecutor(t, numRows, rf)

	provided, err := re.GetRows(numRows * 2)
	require.NoError(t, err)
	require.NotNil(t, provided)
	require.Equal(t, numRows, provided.RowCount())

	arrRows := commontest.RowsToSlice(provided)
	commontest.SortRows(arrRows)
	arrExpectedRows := commontest.RowsToSlice(allRows)
	for i := 0; i < len(arrRows); i++ {
		commontest.RowsEqual(t, *arrExpectedRows[i], *arrRows[i], colTypes)
	}
}

func TestRemoteExecutorGetOne(t *testing.T) {
	numRows := 100
	rf := common.NewRowsFactory(colTypes)
	re, _, _ := setupRowExecutor(t, numRows, rf)

	provided, err := re.GetRows(1)
	require.NoError(t, err)
	require.NotNil(t, provided)
	require.Equal(t, 1, provided.RowCount())
}

func TestRemoteExecutorGetInBatches(t *testing.T) {
	numRows := 100
	rf := common.NewRowsFactory(colTypes)
	re, allRows, _ := setupRowExecutor(t, numRows, rf)

	allReceived := rf.NewRows(numRows)
	for i := 0; i < 10; i++ {
		rowsToGet := numRows / 10
		provided, err := re.GetRows(rowsToGet)
		require.NoError(t, err)
		require.NotNil(t, provided)
		require.Equal(t, rowsToGet, provided.RowCount())
		allReceived.AppendAll(provided)
	}
	require.Equal(t, numRows, allReceived.RowCount())

	// Should be no more rows
	rowsEmpty, err := re.GetRows(10)
	require.NoError(t, err)
	require.Equal(t, 0, rowsEmpty.RowCount())

	arrRows := commontest.RowsToSlice(allReceived)
	commontest.SortRows(arrRows)
	arrExpectedRows := commontest.RowsToSlice(allRows)
	for i := 0; i < len(arrRows); i++ {
		commontest.RowsEqual(t, *arrExpectedRows[i], *arrRows[i], colTypes)
	}
}

func TestRemoteExecutorSystemTablesTableDoesNotFanout(t *testing.T) {
	allShardsIds := make([]uint64, 10)
	for i := 0; i < 10; i++ {
		allShardsIds[i] = uint64(i)
	}
	tc := &testCluster{allShardIds: allShardsIds}

	re := NewRemoteExecutor(nil, &cluster.QueryExecutionInfo{Query: fmt.Sprintf("select * from %s ", meta.TableDefTableName)},
		colNames, colTypes, "sys", tc, nil)
	require.NotNil(t, re.singlePointGetQueryInfo)
	require.Equal(t, re.singlePointGetQueryInfo.ShardID, cluster.SystemSchemaShardID)

	re = NewRemoteExecutor(nil, &cluster.QueryExecutionInfo{}, colNames, colTypes, "sys", tc, nil)
	require.Len(t, re.clusterGetters, len(allShardsIds))
}

//nolint: unparam
func setupRowExecutor(t *testing.T, numRows int, rf *common.RowsFactory) (PullExecutor, *common.Rows, *testCluster) {
	t.Helper()
	allShardsIds := make([]uint64, 10)
	for i := 0; i < 10; i++ {
		allShardsIds[i] = uint64(i)
	}
	tc := &testCluster{allShardIds: allShardsIds}

	sh := sharder.NewSharder(tc)
	err := sh.Start()
	require.NoError(t, err)

	allRows := rf.NewRows(numRows)
	for i := 0; i < numRows; i++ {
		generateRow(t, i, allRows)
	}
	rowsByShard := map[uint64]*common.Rows{}
	for i := 0; i < numRows; i++ {
		row := allRows.GetRow(i)
		var keyBytes []byte
		// PK is 0th column
		keyBytes = common.AppendUint64ToBufferLE(keyBytes, uint64(row.GetInt64(0)))
		shardID, err := sh.CalculateShard(sharder.ShardTypeHash, keyBytes)
		require.NoError(t, err)
		rows, ok := rowsByShard[shardID]
		if !ok {
			rows = rf.NewRows(1)
			rowsByShard[shardID] = rows
		}
		rows.AppendRow(row)
	}

	tc.rowsByShardOrig = rowsByShard
	tc.reset()

	queryInfo := &cluster.QueryExecutionInfo{}

	return NewRemoteExecutor(nil, queryInfo, colNames, colTypes, "test-schema", tc, nil), allRows, tc
}

func generateRow(t *testing.T, index int, rows *common.Rows) {
	t.Helper()
	rows.AppendInt64ToColumn(0, int64(index))
	rows.AppendStringToColumn(1, fmt.Sprintf("some-place-%d", index))
	rows.AppendFloat64ToColumn(2, 13.567+float64(index))
	dec, err := common.NewDecFromFloat64(13654.567 + float64(index))
	require.NoError(t, err)
	rows.AppendDecimalToColumn(3, *dec)
}

type testCluster struct {
	lock            sync.Mutex
	allShardIds     []uint64
	rowsByShard     map[uint64]*common.Rows
	rowsByShardOrig map[uint64]*common.Rows
}

func (t *testCluster) LocalIterator(startKeyPrefix []byte, endKeyPrefix []byte) cluster.KVIterator {
	panic("implement me")
}

func (t *testCluster) TableDropped(tableID uint64) {
	panic("implement me")
}

func (t *testCluster) GetLeadersMap() (map[uint64]uint64, error) {
	panic("implement me")
}

func (t *testCluster) RegisterStartFill(expectedLeaders map[uint64]uint64, interruptor *interruptor.Interruptor) error {
	panic("implement me")
}

func (t *testCluster) RegisterEndFill() {
	panic("implement me")
}

func (t *testCluster) GetShardAllocs() map[uint64][]int {
	panic("implement me")
}

func (t *testCluster) ExecuteForwardBatch(shardID uint64, batch []byte) error {
	panic("implement me")
}

func (t *testCluster) WriteForwardBatch(batch *cluster.WriteBatch, direct bool, fill bool) error {
	panic("implement me")
}

func (t *testCluster) LinearizableGet(shardID uint64, key []byte) ([]byte, error) {
	return nil, nil
}

func (t *testCluster) SyncStore() error {
	return nil
}

func (t *testCluster) DeleteAllDataInRangeForAllShardsLocally(startPrefix []byte, endPrefix []byte) error {
	return nil
}

func (t *testCluster) PostStartChecks(queryExec common.SimpleQueryExec) error {
	return nil
}

func (t *testCluster) AddToDeleteBatch(batch *cluster.ToDeleteBatch) error {
	return nil
}

func (t *testCluster) RemoveToDeleteBatch(batch *cluster.ToDeleteBatch) error {
	return nil
}

func (t *testCluster) WriteBatchLocally(batch *cluster.WriteBatch) error {
	return nil
}

func (t *testCluster) DeleteAllDataInRangeForShardLocally(shardID uint64, startPrefix []byte, endPrefix []byte) error {
	return nil
}

func (t *testCluster) CreateSnapshot() (cluster.Snapshot, error) {
	return nil, nil
}

func (t *testCluster) LocalScanWithSnapshot(snapshot cluster.Snapshot, startKeyPrefix []byte, endKeyPrefix []byte, limit int) ([]cluster.KVPair, error) {
	return nil, nil
}

func (t *testCluster) GetLock(prefix string) (bool, error) {
	return false, nil
}

func (t *testCluster) ReleaseLock(prefix string) (bool, error) {
	return false, nil
}

func (t *testCluster) reset() {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.rowsByShard = map[uint64]*common.Rows{}
	for k, v := range t.rowsByShardOrig {
		t.rowsByShard[k] = v
	}
}

func (t *testCluster) DeleteAllDataInRangeForAllShards(startPrefix []byte, endPrefix []byte) error {
	panic("should not be called")
}

func (t *testCluster) DeleteAllDataInRangeForShard(shardID uint64, startPrefix []byte, endPrefix []byte) error {
	panic("should not be called")
}

func (t *testCluster) WriteBatch(batch *cluster.WriteBatch, localOnly bool) error {
	panic("should not be called")
}

func (t *testCluster) LocalGet(key []byte) ([]byte, error) {
	panic("should not be called")
}

func (t *testCluster) LocalScan(startKeyPrefix []byte, whileKeyPrefix []byte, limit int) ([]cluster.KVPair, error) {
	panic("should not be called")
}

func (t *testCluster) GetNodeID() int {
	panic("should not be called")
}

func (t *testCluster) GetAllShardIDs() []uint64 {
	return t.allShardIds
}

func (t *testCluster) GetLocalShardIDs() []uint64 {
	panic("should not be called")
}

func (t *testCluster) GenerateClusterSequence(sequenceName string) (uint64, error) {
	panic("should not be called")
}

func (t *testCluster) ExecuteRemotePullQuery(queryInfo *cluster.QueryExecutionInfo, rowsFactory *common.RowsFactory) (*common.Rows, error) {
	t.lock.Lock()
	defer t.lock.Unlock()
	rows := t.rowsByShard[queryInfo.ShardID]
	rowsNew := rowsFactory.NewRows(1)
	rowsToSend := rowsFactory.NewRows(1)
	for i := 0; i < rows.RowCount(); i++ {
		row := rows.GetRow(i)
		if i < int(queryInfo.Limit) {
			rowsToSend.AppendRow(row)
		} else {
			rowsNew.AppendRow(row)
		}
	}
	t.rowsByShard[queryInfo.ShardID] = rowsNew
	return rowsToSend, nil
}

func (t *testCluster) SetRemoteQueryExecutionCallback(callback cluster.RemoteQueryExecutionCallback) {
	panic("should not be called")
}

func (t *testCluster) RegisterShardListenerFactory(factory cluster.ShardListenerFactory) {
	panic("should not be called")
}

func (t *testCluster) BroadcastOneway(notification remoting.ClusterMessage) error {
	panic("should not be called")
}

func (t *testCluster) RegisterMessageHandler(notificationType remoting.ClusterMessageType, listener remoting.ClusterMessageHandler) {
	panic("should not be called")
}

func (t *testCluster) Start() error {
	panic("should not be called")
}

func (t *testCluster) Stop() error {
	panic("should not be called")
}
