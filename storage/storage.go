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
	puts    []KVPair
	deletes [][]byte
}

func (wb *WriteBatch) AddPut(kvPair KVPair) {
	wb.puts = append(wb.puts, kvPair)
}

func (wb *WriteBatch) AddDelete(key []byte) {
	wb.deletes = append(wb.deletes, key)
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
	scan(partitionID uint64, startKeyPrefix []byte, endKeyPrefix []byte, limit int) ([]KVPair, error)
}

type storage struct {
	kvStore kv.KV
	raft    raft.Raft
}

func (s storage) writeBatch(partitionID uint64, batch *WriteBatch, localLeader bool) error {
	panic("implement me")
}

func (s storage) installExecutors(partitionID uint64, plan *ExecutorPlan) {
	panic("implement me")
}

func (s storage) get(partitionID uint64, key []byte) ([]byte, error) {
	panic("implement me")
}

func (s storage) scan(partitionID uint64, startKeyPrefix []byte, endKeyPrefix []byte, limit int) ([]KVPair, error) {
	panic("implement me")
}

func NewStorage(kvStore kv.KV, raft raft.Raft) Storage {
	return &storage{
		kvStore: kvStore,
		raft:    raft,
	}
}
