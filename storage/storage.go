package storage

import (
	"github.com/squareup/pranadb/kv"
	"github.com/squareup/pranadb/raft"
)

type KVPair struct {
	Key []byte
	Value []byte
}

type WriteBatch struct {
	puts []KVPair
	deletes [][]byte
}

type ExecutorPlan struct {

}

// Reliable storage implementation
type Storage interface {

	// Writes a batch reliability to a quorum - goes through the raft layer
	writeBatch(partitionID uint64, batch *WriteBatch, localLeader bool) error

	// Installs executors on the leader for the partition
	// These automatically move if the leader moves
	installExecutors(partitionID uint64, plan *ExecutorPlan)

	// Can read from follower
	get(partitionID uint64, key []byte) ([]byte, error)

	// Can read from follower
	scan(partitionID uint64, startKeyPrefix []byte, endKeyPrefix []byte, limit int) ([][]byte, error)
}

type storage struct {
	kvStore kv.KV
	raft raft.Raft
}

func NewStorage(kvStore kv.KV, raft raft.Raft) Storage {
	return &storage{
		kvStore: kvStore,
		raft:    raft,
	}
}

func (s *storage) installExecutors(partitionID uint64, plan *ExecutorPlan) {
	panic("implement me")
}

func (s *storage) writeBatch(partitionNumber uint64, batch *WriteBatch, localLeader bool) error {
	panic("implement me")
}

func (s *storage) get(partitionNumber uint64, key []byte) ([]byte, error) {
	panic("implement me")
}

func (s *storage) scan(partitionNumber uint64, startKeyPrefix []byte, endKeyPrefix []byte, limit int) ([][]byte, error) {
	panic("implement me")
}
