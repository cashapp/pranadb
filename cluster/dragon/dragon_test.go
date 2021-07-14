package dragon

import (
	"errors"
	"fmt"
	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/protos/squareup/cash/pranadb/notifications"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"
)

var dragonCluster []cluster.Cluster

const (
	numShards = 10
)

func TestMain(m *testing.M) {
	dataDir, err := ioutil.TempDir("", "dragon-test")
	if err != nil {
		panic("failed to create temp dir")
	}
	dragonCluster, err = startDragonCluster(dataDir)
	if err != nil {
		panic(fmt.Sprintf("failed to start dragon cluster %v", err))
	}
	defer func() {
		stopDragonCluster()
		defer func() {
			// We want to do this even if stopDragonCluster fails hence the extra defer
			err := os.RemoveAll(dataDir)
			if err != nil {
				panic("failed to remove test dir")
			}
		}()
	}()
	m.Run()
}

func getLocalNodeAndLocalShard() (cluster.Cluster, uint64) {
	clust := dragonCluster[0]
	shardID := clust.GetLocalShardIDs()[0]
	return clust, shardID
}

func TestLocalPutGet(t *testing.T) {
	node, localShard := getLocalNodeAndLocalShard()

	key := []byte("somekey")
	value := []byte("somevalue")

	kvPair := cluster.KVPair{
		Key:   key,
		Value: value,
	}

	writeBatch := createWriteBatchWithPuts(localShard, kvPair)

	err := node.WriteBatch(&writeBatch)
	require.NoError(t, err)

	res, err := node.LocalGet(key)
	require.NoError(t, err)
	require.NotNil(t, res)

	require.Equal(t, string(value), string(res))
}

func TestLocalPutDelete(t *testing.T) {
	node, localShard := getLocalNodeAndLocalShard()

	key := []byte("somekey")
	value := []byte("somevalue")

	kvPair := cluster.KVPair{
		Key:   key,
		Value: value,
	}

	writeBatch := createWriteBatchWithPuts(localShard, kvPair)

	err := node.WriteBatch(&writeBatch)
	require.NoError(t, err)

	res, err := node.LocalGet(key)
	require.NoError(t, err)
	require.NotNil(t, res)

	deleteBatch := createWriteBatchWithDeletes(localShard, key)

	err = node.WriteBatch(&deleteBatch)
	require.NoError(t, err)

	res, err = node.LocalGet(key)
	require.NoError(t, err)
	require.Nil(t, res)
}

func TestLocalScan(t *testing.T) {
	testLocalScan(t, -1, 10)
}

func TestLocalScanSmallLimit(t *testing.T) {
	testLocalScan(t, 3, 3)
}

func TestLocalScanBigLimit(t *testing.T) {
	testLocalScan(t, 1000, 10)
}

func testLocalScan(t *testing.T, limit int, expected int) {
	t.Helper()
	node, localShard := getLocalNodeAndLocalShard()

	var kvPairs []cluster.KVPair
	for i := 0; i < 10; i++ {
		for j := 0; j < 10; j++ {
			k := []byte(fmt.Sprintf("foo-%02d/bar-%02d", i, j))
			v := []byte(fmt.Sprintf("somevalue%02d", j))
			kvPairs = append(kvPairs, cluster.KVPair{Key: k, Value: v})
		}
	}
	rand.Shuffle(len(kvPairs), func(i, j int) {
		kvPairs[i], kvPairs[j] = kvPairs[j], kvPairs[i]
	})

	wb := cluster.NewWriteBatch(localShard, false)
	for _, kvPair := range kvPairs {
		wb.AddPut(kvPair.Key, kvPair.Value)
	}

	err := node.WriteBatch(wb)
	require.NoError(t, err)

	keyStart := []byte("foo-06")
	keyWhile := []byte("foo-06")

	var res []cluster.KVPair
	res, err = node.LocalScan(keyStart, keyWhile, limit)
	require.NoError(t, err)

	require.Equal(t, expected, len(res))
	for i, kvPair := range res {
		log.Printf("key is: %s", string(kvPair.Key))
		expectedK := fmt.Sprintf("foo-06/bar-%02d", i)
		expectedV := fmt.Sprintf("somevalue%02d", i)
		require.Equal(t, expectedK, string(kvPair.Key))
		require.Equal(t, expectedV, string(kvPair.Value))
	}
}

func TestGenerateTableID(t *testing.T) {
	for i := 0; i < 10; i++ {
		id, err := dragonCluster[i%len(dragonCluster)].GenerateTableID()
		require.NoError(t, err)
		require.Equal(t, uint64(i)+cluster.UserTableIDBase, id)
	}
}

func TestGetNodeID(t *testing.T) {
	for i := 0; i < len(dragonCluster); i++ {
		require.Equal(t, i, dragonCluster[i].GetNodeID())
	}
}

func TestGetAllShardIDs(t *testing.T) {
	for i := 0; i < len(dragonCluster); i++ {
		allShardIds := dragonCluster[i].GetAllShardIDs()
		require.Equal(t, numShards, len(allShardIds))
	}
}

type testNotificationListener struct {
	lock   sync.Mutex
	notifs []cluster.Notification
}

func (t *testNotificationListener) HandleNotification(notification cluster.Notification) {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.notifs = append(t.notifs, notification)
}

func (t *testNotificationListener) getNotifs() []cluster.Notification {
	t.lock.Lock()
	defer t.lock.Unlock()
	return t.notifs
}

func (t *testNotificationListener) clearNotifs() {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.notifs = make([]cluster.Notification, 0)
}

func TestNotifications(t *testing.T) {
	var notifListeners []*testNotificationListener
	for i := 0; i < len(dragonCluster); i++ {
		notifListener := testNotificationListener{notifs: []cluster.Notification{}}
		notifListeners = append(notifListeners, &notifListener)
		dragonCluster[i].RegisterNotificationListener(cluster.NotificationTypeDDLStatement, &notifListener)
	}
	sequences := []uint64{100, 101}
	numNotifs := 10
	for i := 0; i < len(dragonCluster); i++ {
		sendingNode := dragonCluster[i]
		for j := 0; j < numNotifs; j++ {
			notif := &notifications.DDLStatementInfo{
				OriginatingNodeId: int64(sendingNode.GetNodeID()),
				Sequence:          int64(j),
				SchemaName:        "some-schema",
				Sql:               fmt.Sprintf("sql-%d-%d", i, j),
				TableSequences:    sequences,
			}
			err := sendingNode.BroadcastNotification(notif)
			require.NoError(t, err)
		}
		for j := 0; j < len(dragonCluster); j++ {
			listener := notifListeners[j]
			common.WaitUntil(t, func() (bool, error) {
				lNotifs := len(listener.getNotifs())
				return lNotifs == numNotifs, nil
			})
			for k := 0; k < numNotifs; k++ {
				notif := listener.getNotifs()[k]
				ddlStmt, ok := notif.(*notifications.DDLStatementInfo)
				require.True(t, ok)
				require.Equal(t, sendingNode.GetNodeID(), int(ddlStmt.OriginatingNodeId))
				require.Equal(t, int64(k), ddlStmt.Sequence)
				require.Equal(t, "some-schema", ddlStmt.SchemaName)
				require.Equal(t, fmt.Sprintf("sql-%d-%d", sendingNode.GetNodeID(), k), ddlStmt.Sql)
				require.Equal(t, len(sequences), len(ddlStmt.TableSequences))
				for l := 0; l < len(sequences); l++ {
					require.Equal(t, sequences[l], ddlStmt.TableSequences[l])
				}
			}
			listener.clearNotifs()
		}
	}
}

func stopDragonCluster() {
	for _, dragon := range dragonCluster {
		err := dragon.Stop()
		if err != nil {
			panic(fmt.Sprintf("failed to stop dragon cluster %v", err))
		}
	}
}

func startDragonCluster(dataDir string) ([]cluster.Cluster, error) {

	nodeAddresses := []string{
		"localhost:63101",
		"localhost:63102",
		"localhost:63103",
	}

	chans := make([]chan error, len(nodeAddresses))
	clusterNodes := make([]cluster.Cluster, len(nodeAddresses))
	for i := 0; i < len(chans); i++ {
		ch := make(chan error)
		chans[i] = ch
		dragon, err := NewDragon(i, 123, nodeAddresses, numShards, dataDir, 3, true)
		if err != nil {
			return nil, err
		}
		clusterNodes[i] = dragon
		dragon.RegisterShardListenerFactory(&cluster.DummyShardListenerFactory{})
		dragon.SetRemoteQueryExecutionCallback(&cluster.DummyRemoteQueryExecutionCallback{})

		go startDragonNode(dragon, ch)
	}

	for i := 0; i < len(chans); i++ {
		err, ok := <-chans[i]
		if !ok {
			return nil, errors.New("channel was closed")
		}
		if err != nil {
			return nil, err
		}
	}

	time.Sleep(5 * time.Second)
	return clusterNodes, nil
}

func startDragonNode(dragon cluster.Cluster, ch chan error) {
	err := dragon.Start()
	ch <- err
}

func createWriteBatchWithPuts(shardID uint64, puts ...cluster.KVPair) cluster.WriteBatch {
	wb := cluster.NewWriteBatch(shardID, false)
	for _, kvPair := range puts {
		wb.AddPut(kvPair.Key, kvPair.Value)
	}
	return *wb
}

func createWriteBatchWithDeletes(shardID uint64, deletes ...[]byte) cluster.WriteBatch {
	wb := cluster.NewWriteBatch(shardID, false)
	for _, del := range deletes {
		wb.AddDelete(del)
	}
	return *wb
}
