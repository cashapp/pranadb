package cluster

import (
	"github.com/squareup/pranadb/common"
)

// WriteBatch represents some puts and deletes that will be written atomically by the underlying storage implementation
// It is not thread safe
type WriteBatch struct {
	ShardID      uint64
	puts         *common.ByteSliceMap
	Deletes      *common.ByteSliceMap
	NotifyRemote bool
}

func NewWriteBatch(shardID uint64, notifyRemote bool) *WriteBatch {
	return &WriteBatch{
		ShardID:      shardID,
		puts:         common.NewByteSliceMap(),
		Deletes:      common.NewByteSliceMap(),
		NotifyRemote: notifyRemote,
	}
}

func (wb *WriteBatch) AddPut(k []byte, v []byte) {
	wb.puts.Put(k, v)
}

func (wb *WriteBatch) AddDelete(k []byte) {
	wb.Deletes.Put(k, nil)
}

func (wb *WriteBatch) HasWrites() bool {
	return len(wb.puts.TheMap) > 0 || len(wb.Deletes.TheMap) > 0
}

func (wb *WriteBatch) Serialize(buff []byte) []byte {
	buff = common.AppendUint32ToBufferLE(buff, uint32(len(wb.puts.TheMap)))
	for k, v := range wb.puts.TheMap {
		kb := common.StringToByteSliceZeroCopy(k)
		buff = common.AppendUint32ToBufferLE(buff, uint32(len(kb)))
		buff = append(buff, kb...)
		buff = common.AppendUint32ToBufferLE(buff, uint32(len(v)))
		buff = append(buff, v...)
	}
	buff = common.AppendUint32ToBufferLE(buff, uint32(len(wb.Deletes.TheMap)))
	for k := range wb.Deletes.TheMap {
		kb := common.StringToByteSliceZeroCopy(k)
		buff = common.AppendUint32ToBufferLE(buff, uint32(len(kb)))
		buff = append(buff, kb...)
	}
	return buff
}
