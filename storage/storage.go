package storage

import (
	"github.com/squareup/pranadb/kv"
	"github.com/squareup/pranadb/raft"
)

type KVPair struct {
	Key   []byte
	Value []byte
}

type WriteBatch struct {
	ShardID uint64
	puts    []KVPair
	deletes [][]byte
}

func NewWriteBatch(shardID uint64) *WriteBatch {
	return &WriteBatch{ShardID: shardID}
}

func (wb *WriteBatch) AddPut(kvPair KVPair) {
	wb.puts = append(wb.puts, kvPair)
}

func (wb *WriteBatch) AddDelete(key []byte) {
	wb.deletes = append(wb.deletes, key)
}

func (wb *WriteBatch) HasWrites() bool {
	return len(wb.puts) > 0 || len(wb.deletes) > 0
}

type ExecutorPlan struct {
}

type Storage interface {

	// WriteBatch writes a batch reliability to a quorum - goes through the raft layer
	WriteBatch(batch *WriteBatch, localLeader bool) error

	// InstallExecutors installs executors on the leader for the partition
	// These automatically move if the leader moves
	InstallExecutors(shardID uint64, plan *ExecutorPlan)

	// Get can read from follower
	Get(shardID uint64, key []byte, localLeader bool) ([]byte, error)

	// Scan can read from follower
	Scan(shardID uint64, startKeyPrefix []byte, endKeyPrefix []byte, limit int) ([]KVPair, error)

	AddShard(shardID uint64, callback ShardCallback) error

	RemoveShard(shardID uint64) error

	GetClusterInfo() (*ClusterInfo, error)

	GetNodeInfo(nodeID int) (*NodeInfo, error)

	// GenerateTableID generates a table if using a cluster wide persistent counter
	GenerateTableID() (uint64, error)
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

type ShardCallback interface {
	Write(batch WriteBatch) error
}

type storage struct {
	kvStore kv.KV
	raft    raft.Raft
}

func (s storage) GenerateTableID() (uint64, error) {
	panic("implement me")
}

func (s storage) Get(shardID uint64, key []byte, localLeader bool) ([]byte, error) {
	panic("implement me")
}

func (s storage) GetClusterInfo() (*ClusterInfo, error) {
	panic("implement me")
}

func (s storage) GetNodeInfo(nodeID int) (*NodeInfo, error) {
	panic("implement me")
}

func (s storage) WriteBatch(batch *WriteBatch, localLeader bool) error {
	panic("implement me")
}

func (s storage) InstallExecutors(groupID uint64, plan *ExecutorPlan) {
	panic("implement me")
}

func (s storage) Scan(shardID uint64, startKeyPrefix []byte, endKeyPrefix []byte, limit int) ([]KVPair, error) {
	panic("implement me")
}

func (s storage) AddShard(shardID uint64, callback ShardCallback) error {
	panic("implement me")
}

func (s storage) RemoveShard(shardID uint64) error {
	panic("implement me")
}

func NewStorage(kvStore kv.KV, raft raft.Raft) Storage {
	return &storage{
		kvStore: kvStore,
		raft:    raft,
	}
}
