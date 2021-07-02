package cluster

import "github.com/squareup/pranadb/common"

// Cluster manages cluster wide information and communication
type Cluster interface {
	GetNodeID() int

	GetClusterInfo() (*ClusterInfo, error)

	GetNodeInfo(nodeID int) (*NodeInfo, error)

	// GenerateTableID generates a table using a cluster wide persistent counter
	GenerateTableID() (uint64, error)

	SetLeaderChangedCallback(callback LeaderChangeCallback)

	ExecuteRemotePullQuery(schemaName string, query string, queryID string, limit int, nodeID int) chan RemoteQueryResult

	SetRemoteQueryExecutionCallback(callback RemoteQueryExecutionCallback)

	Start() error

	Stop() error
}

type RemoteQueryResult struct {
	Rows *common.Rows
	Err  error
}

type LeaderChangeCallback interface {
	LeaderChanged(shardID uint64, added bool)
}

type RemoteQueryExecutionCallback interface {
	ExecuteRemotePullQuery(schemaName string, query string, queryID string, limit int) (*common.Rows, error)
}

// ClusterInfo describes the cluster in terms of which nodes have which shards, both leaders and followers
type ClusterInfo struct {
	// Map of node id to NodeInfo
	NodeInfos map[int]*NodeInfo
}

type NodeInfo struct {
	Leaders   []uint64
	Followers []uint64
}
