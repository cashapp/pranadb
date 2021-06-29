package cluster

import (
	"fmt"
	"sync"
)

type fakeClusterManager struct {
	mu            sync.RWMutex
	clusterInfo   *ClusterInfo
	tableSequence uint64
}

func (f *fakeClusterManager) GetNodeID() int {
	panic("implement me")
}

func NewFakeClusterManager(nodeID int, numShards int) Cluster {
	return &fakeClusterManager{
		clusterInfo:   createClusterInfo(nodeID, numShards),
		tableSequence: uint64(100), // First 100 reserved for system tables
	}
}

func (f *fakeClusterManager) GenerateTableID() (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	res := f.tableSequence
	f.tableSequence++
	return res, nil
}

func (f *fakeClusterManager) GetClusterInfo() (*ClusterInfo, error) {
	return f.clusterInfo, nil
}

func (f *fakeClusterManager) GetNodeInfo(nodeID int) (*NodeInfo, error) {
	nodeInfo, ok := f.clusterInfo.NodeInfos[nodeID]
	if !ok {
		return nil, fmt.Errorf("Invalid node id %d", nodeID)
	}
	return nodeInfo, nil
}

func (f *fakeClusterManager) Start() error {
	return nil
}

func (f *fakeClusterManager) Stop() error {
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
