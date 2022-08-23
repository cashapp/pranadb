package integration

import (
	"time"

	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/cluster/dragon"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/conf"
	"github.com/squareup/pranadb/errors"
)

// TestCluster is a DragonBoat cluster for integration testing.
type TestCluster struct {
	dataDir           string
	numShards         int
	replicationFactor int
	tlsConfig         conf.TLSConfig
	nodes             []*dragon.Dragon
}

// NewTestCluster creates a new DragonBoat integration test cluster.
func NewTestCluster(dataDir string, numShards int, replicationFactor int, tlsConfig conf.TLSConfig) *TestCluster {
	return &TestCluster{
		dataDir:           dataDir,
		numShards:         numShards,
		replicationFactor: replicationFactor,
		tlsConfig:         tlsConfig,
	}
}

// Start starts a DragonBoat cluster for integration testing
func (c *TestCluster) Start() error {
	nodeAddresses := []string{
		"localhost:63101",
		"localhost:63102",
		"localhost:63103",
	}

	chans := make([]chan error, len(nodeAddresses))
	c.nodes = make([]*dragon.Dragon, len(nodeAddresses))
	for i := 0; i < len(chans); i++ {
		ch := make(chan error)
		chans[i] = ch
		cnf := conf.NewDefaultConfig()
		cnf.NodeID = i
		cnf.ClusterID = 123
		cnf.RaftListenAddresses = nodeAddresses
		cnf.NumShards = c.numShards
		cnf.DataDir = c.dataDir
		cnf.ReplicationFactor = c.replicationFactor
		cnf.TestServer = true
		cnf.IntraClusterTLSConfig = c.tlsConfig
		clus, err := dragon.NewDragon(*cnf, &common.AtomicBool{})
		if err != nil {
			return errors.WithStack(err)
		}
		c.nodes[i] = clus
		clus.RegisterShardListenerFactory(&cluster.DummyShardListenerFactory{})
		clus.SetRemoteQueryExecutionCallback(&cluster.DummyRemoteQueryExecutionCallback{})

		go startNode(clus, ch)
	}

	for i := 0; i < len(chans); i++ {
		err, ok := <-chans[i]
		if !ok {
			return errors.Error("channel was closed")
		}
		if err != nil {
			return errors.WithStack(err)
		}
	}

	time.Sleep(5 * time.Second)
	return nil
}

// Stop stops the test cluster.
func (c *TestCluster) Stop() error {
	for _, node := range c.nodes {
		err := node.Stop()
		if err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

// GetLocalNodeAndShard retrieves the local node and shard.
func (c *TestCluster) GetLocalNodeAndShard() (*dragon.Dragon, uint64) {
	clust := c.nodes[0]
	shardID := clust.GetLocalShardIDs()[0]
	return clust, shardID
}

func startNode(clus cluster.Cluster, ch chan error) {
	err := clus.Start()
	ch <- err
}
