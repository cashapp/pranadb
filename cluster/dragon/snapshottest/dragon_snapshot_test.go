package snapshottest

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/cluster/dragon"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/conf"
	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/table"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"testing"
	"time"
)

func TestSnapshot(t *testing.T) {
	dataDir, err := ioutil.TempDir("", "dragon-test")
	if err != nil {
		panic("failed to create temp dir")
	}
	// We start just two nodes of the three node cluster - this provides a quorum so they will accept writes
	dragonCluster, err := startDragonNodes(dataDir, 0, 2)
	if err != nil {
		panic(fmt.Sprintf("failed to start dragon cluster %+v", err))
	}
	node := dragonCluster[0]

	// Write a bunch of data to a shard
	numKeys := 50
	batchSize := 1
	var wb *cluster.WriteBatch
	shardID := node.GetAllShardIDs()[0]
	tableID := uint64(1056)
	var keys [][]byte
	var vals []string
	for i := 0; i < numKeys; i++ {
		skey := fmt.Sprintf("some-key-%06d", i)
		value := fmt.Sprintf("some-value-%d", i)
		key := table.EncodeTableKeyPrefix(tableID, shardID, 16)
		key = append(key, []byte(skey)...)
		keys = append(keys, key)
		vals = append(vals, value)
		if wb == nil {
			wb = cluster.NewWriteBatch(shardID)
		}
		wb.AddPut(key, []byte(value))
		if i%batchSize == 0 {
			err := node.WriteBatch(wb, false)
			require.NoError(t, err)
			log.Info("wrote batch")
			wb = nil
		}
	}
	if wb != nil {
		err := node.WriteBatch(wb, false)
		require.NoError(t, err)
		log.Info("wrote batch")
	}

	// At this point no snapshots should have been done.
	// We use an IOnDiskStateMachine and Dragon will NOT create snapshots periodically because the state machine
	// is persisted to disk anyway (that's not true for standard state machines)
	// Snapshots are only created when another node starts and needs to get its state machine in sync with the rest
	// of the group. In this case a snapshot stream is requested from an existing node, the snapshot is created and
	// streamed to the new node
	require.Equal(t, int64(0), dragonCluster[0].SaveSnapshotCount())
	require.Equal(t, int64(0), dragonCluster[0].RestoreSnapshotCount())
	require.Equal(t, int64(0), dragonCluster[2].SaveSnapshotCount())
	require.Equal(t, int64(0), dragonCluster[2].RestoreSnapshotCount())

	// Now start the another node
	// This should trigger a snapshot on one of the existing nodes which should then be streamed to the new node
	// so it can get up to state
	dragonCluster2, err := startDragonNodes(dataDir, 1)
	if err != nil {
		panic(fmt.Sprintf("failed to start dragon cluster %+v", err))
	}
	require.Equal(t, int64(0), dragonCluster2[1].SaveSnapshotCount())
	require.Equal(t, int64(1), dragonCluster2[1].RestoreSnapshotCount())

	require.Equal(t, int64(0), dragonCluster[0].RestoreSnapshotCount())
	require.Equal(t, int64(0), dragonCluster[2].RestoreSnapshotCount())

	require.True(t, dragonCluster[0].SaveSnapshotCount() == 1 || dragonCluster[2].SaveSnapshotCount() == 1)
	require.False(t, dragonCluster[0].SaveSnapshotCount() == 1 && dragonCluster[2].SaveSnapshotCount() == 1)

	// Now we read the data back from this node
	// We read it directly from Pebble to make sure it's actually got to that node
	for i, key := range keys {
		va, err := dragonCluster2[1].LocalGet(key)
		require.NoError(t, err)
		require.Equal(t, vals[i], string(va))
	}

	stopDragonCluster(dragonCluster)
	stopDragonCluster(dragonCluster2)
}

func startDragonNodes(dataDir string, nodes ...int) ([]*dragon.Dragon, error) {

	nodeAddresses := []string{
		"localhost:63101",
		"localhost:63102",
		"localhost:63103",
	}

	requiredNodes := map[int]struct{}{}
	for _, node := range nodes {
		requiredNodes[node] = struct{}{}
	}

	var chans []chan error
	clusterNodes := make([]*dragon.Dragon, len(nodeAddresses))
	for i := 0; i < len(nodeAddresses); i++ {
		_, ok := requiredNodes[i]
		if !ok {
			continue
		}
		ch := make(chan error)
		chans = append(chans, ch)
		cnf := conf.NewDefaultConfig()
		cnf.NodeID = i
		cnf.ClusterID = 123
		cnf.RaftAddresses = nodeAddresses
		cnf.NumShards = 10
		cnf.DataDir = dataDir
		cnf.ReplicationFactor = 3
		cnf.TestServer = true
		cnf.DataSnapshotEntries = 10
		cnf.DataCompactionOverhead = 5
		clus, err := dragon.NewDragon(*cnf, &common.AtomicBool{})
		if err != nil {
			return nil, err
		}
		clusterNodes[i] = clus
		clus.RegisterShardListenerFactory(&cluster.DummyShardListenerFactory{})
		clus.SetRemoteQueryExecutionCallback(&cluster.DummyRemoteQueryExecutionCallback{})

		go startDragonNode(clus, ch)
	}

	for _, ch := range chans {
		err, ok := <-ch
		if !ok {
			return nil, errors.Error("channel was closed")
		}
		if err != nil {
			return nil, errors.WithStack(err)
		}
	}

	time.Sleep(5 * time.Second)
	return clusterNodes, nil
}

func startDragonNode(clus cluster.Cluster, ch chan error) {
	err := clus.Start()
	ch <- err
}

func stopDragonCluster(dragonCluster []*dragon.Dragon) {
	for _, dragon := range dragonCluster {
		if dragon != nil {
			err := dragon.Stop()
			if err != nil {
				panic(fmt.Sprintf("failed to stop dragon cluster %+v", err))
			}
		}
	}
}
