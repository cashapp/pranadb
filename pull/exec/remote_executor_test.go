package exec

import (
	"fmt"
	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/common/commontest"
	"github.com/squareup/pranadb/sharder"
	"github.com/stretchr/testify/require"
	"log"
	"sync"
	"testing"
)

func TestRemoteExecutorGetAll(t *testing.T) {
	numRows := 100
	rf := common.NewRowsFactory(colTypes)
	re, allRows, _ := setupRowExecutor(t, numRows, rf, false)

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
	re, allRows, _ := setupRowExecutor(t, numRows, rf, false)

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
	re, _, _ := setupRowExecutor(t, numRows, rf, false)

	provided, err := re.GetRows(1)
	require.NoError(t, err)
	require.NotNil(t, provided)
	require.Equal(t, 1, provided.RowCount())
}

func TestRemoteExecutorGetInBatches(t *testing.T) {
	numRows := 100
	rf := common.NewRowsFactory(colTypes)
	re, allRows, _ := setupRowExecutor(t, numRows, rf, false)

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

func TestRemoteExecutorResetAndGetAgain(t *testing.T) {
	numRows := 100
	rf := common.NewRowsFactory(colTypes)
	re, allRows, tc := setupRowExecutor(t, numRows, rf, true)

	provided, err := re.GetRows(numRows)
	require.NoError(t, err)
	require.NotNil(t, provided)
	require.Equal(t, numRows, provided.RowCount())

	// Now we try and get them all again - this should work as the executor will reset itself if called again
	// after being complete
	tc.reset()
	re.Reset()
	provided, err = re.GetRows(numRows)
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

//nolint: unparam
func setupRowExecutor(t *testing.T, numRows int, rf *common.RowsFactory, ps bool) (PullExecutor, *common.Rows, *testCluster) {
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
		keyBytes = common.AppendUint64ToBufferLittleEndian(keyBytes, uint64(row.GetInt64(0)))
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

	queryInfo := &cluster.QueryExecutionInfo{
		IsPs: ps,
	}

	return NewRemoteExecutor(nil, queryInfo, colTypes, "test-schema", tc), allRows, tc
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

func (t *testCluster) RegisterMembershipListener(listener cluster.MembershipListener) {
}

func (t *testCluster) reset() {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.rowsByShard = map[uint64]*common.Rows{}
	for k, v := range t.rowsByShardOrig {
		t.rowsByShard[k] = v
	}
}

func (t *testCluster) DeleteAllDataInRange(startPrefix []byte, endPrefix []byte) error {
	panic("should not be called")
}

func (t *testCluster) WriteBatch(batch *cluster.WriteBatch) error {
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

func (t *testCluster) GenerateTableID() (uint64, error) {
	panic("should not be called")
}

func (t *testCluster) ExecuteRemotePullQuery(queryInfo *cluster.QueryExecutionInfo, rowsFactory *common.RowsFactory) (*common.Rows, error) {
	t.lock.Lock()
	defer t.lock.Unlock()
	log.Printf("call to get %d rows from shard %d", queryInfo.Limit, queryInfo.ShardID)
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

func (t *testCluster) BroadcastNotification(notification cluster.Notification) error {
	panic("should not be called")
}

func (t *testCluster) RegisterNotificationListener(notificationType cluster.NotificationType, listener cluster.NotificationListener) {
	panic("should not be called")
}

func (t *testCluster) Start() error {
	panic("should not be called")
}

func (t *testCluster) Stop() error {
	panic("should not be called")
}
