package storage

import (
	"github.com/squareup/pranadb/common"
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

type Storage interface {

	// WriteBatch writes a batch reliability to a quorum - goes through the raft layer
	WriteBatch(batch *WriteBatch, localLeader bool) error

	// Get can read from follower
	Get(shardID uint64, key []byte, localLeader bool) ([]byte, error)

	// Scan scans the local store
	Scan(startKeyPrefix []byte, whileKeyPrefix []byte, limit int) ([]KVPair, error)

	CreateShard(shardID uint64, callback ShardCallback) error

	RemoveShard(shardID uint64) error

	SetRemoteWriteHandler(handler RemoteWriteHandler)

	Start() error

	Stop() error
}

// RemoteWriteHandler will be called when a remote write is done to a shard
type RemoteWriteHandler interface {
	RemoteWriteOccurred(shardID uint64)
}

type ShardCallback interface {
	Write(batch WriteBatch) error
}
