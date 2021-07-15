package cluster

import (
	"fmt"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/protos/squareup/cash/pranadb/notifications"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func startFakeCluster(t *testing.T) Cluster {
	t.Helper()
	clust := NewFakeCluster(0, 10)
	clust.RegisterShardListenerFactory(&DummyShardListenerFactory{})
	clust.SetRemoteQueryExecutionCallback(&DummyRemoteQueryExecutionCallback{})
	err := clust.Start()
	require.NoError(t, err)
	return clust
}

// nolint: unparam
func stopClustFunc(t *testing.T, clust Cluster) {
	t.Helper()
	err := clust.Stop()
	require.NoError(t, err)
}

func TestPutGet(t *testing.T) {

	clust := startFakeCluster(t)
	defer stopClustFunc(t, clust)

	key := []byte("somekey")
	value := []byte("somevalue")

	kvPair := KVPair{
		Key:   key,
		Value: value,
	}

	shardID := uint64(123545)
	writeBatch := createWriteBatchWithPuts(shardID, kvPair)

	err := clust.WriteBatch(&writeBatch)
	require.NoError(t, err)

	res, err := clust.LocalGet(key)
	require.NoError(t, err)
	require.NotNil(t, res)

	require.Equal(t, string(value), string(res))

}

func TestPutDelete(t *testing.T) {

	clust := startFakeCluster(t)
	defer stopClustFunc(t, clust)

	key := []byte("somekey")
	value := []byte("somevalue")

	kvPair := KVPair{
		Key:   key,
		Value: value,
	}

	shardID := uint64(123545)
	writeBatch := createWriteBatchWithPuts(shardID, kvPair)

	err := clust.WriteBatch(&writeBatch)
	require.NoError(t, err)

	res, err := clust.LocalGet(key)
	require.NoError(t, err)
	require.NotNil(t, res)

	deleteBatch := createWriteBatchWithDeletes(shardID, key)

	err = clust.WriteBatch(&deleteBatch)
	require.NoError(t, err)

	res, err = clust.LocalGet(key)
	require.NoError(t, err)
	require.Nil(t, res)
}

func TestScan(t *testing.T) {
	testScan(t, -1, 10)
}

func TestScanSmallLimit(t *testing.T) {
	testScan(t, 3, 3)
}

func TestScanBigLimit(t *testing.T) {
	testScan(t, 1000, 10)
}

func testScan(t *testing.T, limit int, expected int) {
	t.Helper()
	clust := startFakeCluster(t)
	defer stopClustFunc(t, clust)

	var kvPairs []KVPair
	for i := 0; i < 10; i++ {
		for j := 0; j < 10; j++ {
			k := []byte(fmt.Sprintf("foo-%02d/bar-%02d", i, j))
			v := []byte(fmt.Sprintf("somevalue%02d", j))
			kvPairs = append(kvPairs, KVPair{Key: k, Value: v})
		}
	}
	rand.Shuffle(len(kvPairs), func(i, j int) {
		kvPairs[i], kvPairs[j] = kvPairs[j], kvPairs[i]
	})
	shardID := uint64(123545)

	wb := NewWriteBatch(shardID, false)
	for _, kvPair := range kvPairs {
		wb.AddPut(kvPair.Key, kvPair.Value)
	}

	err := clust.WriteBatch(wb)
	require.NoError(t, err)

	keyStart := []byte("foo-06")
	keyWhile := []byte("foo-06")

	var res []KVPair
	res, err = clust.LocalScan(keyStart, keyWhile, limit)
	require.NoError(t, err)

	require.Equal(t, expected, len(res))
	for i, kvPair := range res {
		expectedK := fmt.Sprintf("foo-06/bar-%02d", i)
		expectedV := fmt.Sprintf("somevalue%02d", i)
		require.Equal(t, expectedK, string(kvPair.Key))
		require.Equal(t, expectedV, string(kvPair.Value))
	}
}

func createWriteBatchWithPuts(shardID uint64, puts ...KVPair) WriteBatch {
	wb := NewWriteBatch(shardID, false)
	for _, kvPair := range puts {
		wb.AddPut(kvPair.Key, kvPair.Value)
	}
	return *wb
}

func createWriteBatchWithDeletes(shardID uint64, deletes ...[]byte) WriteBatch {
	wb := NewWriteBatch(shardID, false)
	for _, delete := range deletes {
		wb.AddDelete(delete)
	}
	return *wb
}

func TestNotifications(t *testing.T) {

	clust := startFakeCluster(t)
	defer stopClustFunc(t, clust)

	notifListener := TestNotificationListener{notifs: []Notification{}}
	clust.RegisterNotificationListener(NotificationTypeDDLStatement, &notifListener)
	sequences := []uint64{100, 101}
	numNotifs := 10

	for i := 0; i < numNotifs; i++ {
		notif := &notifications.DDLStatementInfo{
			OriginatingNodeId: int64(clust.GetNodeID()),
			Sequence:          int64(i),
			SchemaName:        "some-schema",
			Sql:               fmt.Sprintf("sql-%d-%d", clust.GetNodeID(), i),
			TableSequences:    sequences,
		}
		err := clust.BroadcastNotification(notif)
		require.NoError(t, err)
	}

	common.WaitUntil(t, func() (bool, error) {
		lNotifs := len(notifListener.getNotifs())
		return lNotifs == numNotifs, nil
	})
	for i := 0; i < numNotifs; i++ {
		notif := notifListener.getNotifs()[i]
		ddlStmt, ok := notif.(*notifications.DDLStatementInfo)
		require.True(t, ok)
		require.Equal(t, clust.GetNodeID(), int(ddlStmt.OriginatingNodeId))
		require.Equal(t, int64(i), ddlStmt.Sequence)
		require.Equal(t, "some-schema", ddlStmt.SchemaName)
		require.Equal(t, fmt.Sprintf("sql-%d-%d", clust.GetNodeID(), i), ddlStmt.Sql)
		require.Equal(t, len(sequences), len(ddlStmt.TableSequences))
		for l := 0; l < len(sequences); l++ {
			require.Equal(t, sequences[l], ddlStmt.TableSequences[l])
		}
	}
}
