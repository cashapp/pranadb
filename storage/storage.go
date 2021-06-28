package storage

import (
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/kv"
	"github.com/squareup/pranadb/raft"
)

type KVPair struct {
	Key   []byte
	Value []byte
}

type WriteBatch struct {
	ShardID uint64
	puts    *common.ByteSliceMap
	deletes *common.ByteSliceMap
}

func NewWriteBatch(shardID uint64) *WriteBatch {
	return &WriteBatch{
		ShardID: shardID,
		puts:    common.NewByteSliceMap(),
		deletes: common.NewByteSliceMap(),
	}
}

func (wb *WriteBatch) AddPut(k []byte, v []byte) {
	wb.puts.Put(k, v)
}

func (wb *WriteBatch) AddDelete(k []byte) {
	wb.deletes.Put(k, nil)
}

func (wb *WriteBatch) HasWrites() bool {
	return len(wb.puts.TheMap) > 0 || len(wb.deletes.TheMap) > 0
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

	// Scan scans the local store
	Scan(startKeyPrefix []byte, whileKeyPrefix []byte, limit int) ([]KVPair, error)

	CreateShard(shardID uint64, callback ShardCallback) error

	RemoveShard(shardID uint64) error

	SetRemoteWriteHandler(handler RemoteWriteHandler)
}

// RemoteWriteHandler will be called when a remote write is done to a shard
type RemoteWriteHandler interface {
	RemoteWriteOccurred(shardID uint64)
}

type ShardCallback interface {
	Write(batch WriteBatch) error
}

type storage struct {
	kvStore kv.KV
	raft    raft.Raft
}

func (s storage) SetRemoteWriteHandler(handler RemoteWriteHandler) {
	panic("implement me")
}

func (s storage) GenerateTableID() (uint64, error) {
	panic("implement me")
}

func (s storage) Get(shardID uint64, key []byte, localLeader bool) ([]byte, error) {
	panic("implement me")
}

func (s storage) WriteBatch(batch *WriteBatch, localLeader bool) error {
	panic("implement me")
}

func (s storage) InstallExecutors(groupID uint64, plan *ExecutorPlan) {
	panic("implement me")
}

func (s storage) Scan(startKeyPrefix []byte, endKeyPrefix []byte, limit int) ([]KVPair, error) {
	panic("implement me")
}

func (s storage) CreateShard(shardID uint64, callback ShardCallback) error {
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
