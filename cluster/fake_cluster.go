package cluster

import (
	"errors"
	"fmt"
	"sync"
)

type fakeCluster struct {
	nodeID                       int
	mu                           sync.RWMutex
	clusterInfo                  *ClusterInfo
	tableSequence                uint64
	leaderChangeCallback         LeaderChangeCallback
	remoteQueryExecutionCallback RemoteQueryExecutionCallback
}

func (f *fakeCluster) ExecuteRemotePullQuery(schemaName string, query string, queryID string, limit int, nodeID int) chan RemoteQueryResult {
	f.mu.Lock()
	callback := f.remoteQueryExecutionCallback
	f.mu.Unlock()
	ch := make(chan RemoteQueryResult, 1)
	if callback != nil {
		go func() {
			rows, err := callback.ExecuteRemotePullQuery(schemaName, query, queryID, limit)
			ch <- RemoteQueryResult{
				Rows: rows,
				Err:  err,
			}
		}()
		return ch
	} else {
		ch <- RemoteQueryResult{
			Err: errors.New("no remote query callback registered"),
		}
	}
	return ch
}

func (f *fakeCluster) SetRemoteQueryExecutionCallback(callback RemoteQueryExecutionCallback) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.remoteQueryExecutionCallback = callback
}

func (f *fakeCluster) SetLeaderChangedCallback(callback LeaderChangeCallback) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.leaderChangeCallback = callback
}

func (f *fakeCluster) GetNodeID() int {
	return f.nodeID
}

func NewFakeClusterManager(nodeID int, numShards int) Cluster {
	return &fakeCluster{
		nodeID:        nodeID,
		clusterInfo:   createClusterInfo(nodeID, numShards),
		tableSequence: uint64(100), // First 100 reserved for system tables
	}
}

func (f *fakeCluster) GenerateTableID() (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	res := f.tableSequence
	f.tableSequence++
	return res, nil
}

func (f *fakeCluster) GetClusterInfo() (*ClusterInfo, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.clusterInfo, nil
}

func (f *fakeCluster) GetNodeInfo(nodeID int) (*NodeInfo, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	nodeInfo, ok := f.clusterInfo.NodeInfos[nodeID]
	if !ok {
		return nil, fmt.Errorf("Invalid node id %d", nodeID)
	}
	return nodeInfo, nil
}

func (f *fakeCluster) Start() error {
	return nil
}

func (f *fakeCluster) Stop() error {
	return nil
}

func createClusterInfo(nodeID int, numShards int) *ClusterInfo {
	leaders := make([]uint64, numShards)
	for i := 0; i < numShards; i++ {
		leaders[i] = uint64(i)
	}
	nodeInfo := &NodeInfo{
		Leaders:   leaders,
		Followers: nil,
	}
	nodeInfos := make(map[int]*NodeInfo)
	nodeInfos[nodeID] = nodeInfo
	return &ClusterInfo{NodeInfos: nodeInfos}
}
