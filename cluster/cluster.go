package cluster

// Cluster keeps track of which nodes have which shards
type Cluster interface {
	GetNodeID() int

	GetClusterInfo() (*ClusterInfo, error)

	GetNodeInfo(nodeID int) (*NodeInfo, error)

	// GenerateTableID generates a table using a cluster wide persistent counter
	GenerateTableID() (uint64, error)

	Start() error

	Stop() error
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
