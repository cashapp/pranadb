package cluster

import "github.com/squareup/pranadb/common"

type WriteBatch struct {
	ShardID      uint64
	Puts         *common.ByteSliceMap
	Deletes      *common.ByteSliceMap
	NotifyRemote bool
}

func NewWriteBatch(shardID uint64, notifyRemote bool) *WriteBatch {
	return &WriteBatch{
		ShardID:      shardID,
		Puts:         common.NewByteSliceMap(),
		Deletes:      common.NewByteSliceMap(),
		NotifyRemote: notifyRemote,
	}
}

func (wb *WriteBatch) AddPut(k []byte, v []byte) {
	wb.Puts.Put(k, v)
}

func (wb *WriteBatch) AddDelete(k []byte) {
	wb.Deletes.Put(k, nil)
}

func (wb *WriteBatch) HasWrites() bool {
	return len(wb.Puts.TheMap) > 0 || len(wb.Deletes.TheMap) > 0
}
