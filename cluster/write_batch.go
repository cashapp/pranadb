package cluster

import (
	"github.com/squareup/pranadb/common"
)

// WriteBatch represents some Puts and deletes that will be written atomically by the underlying storage implementation
// It is not thread safe
type WriteBatch struct {
	ShardID            uint64
	Puts               *common.ByteSliceMap
	Deletes            *common.ByteSliceMap
	NotifyRemote       bool
	HasForwards        bool
	committedCallbacks []CommittedCallback
}

type CommittedCallback func() error

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

func (wb *WriteBatch) Serialize(buff []byte) []byte {
	buff = common.AppendUint32ToBufferLE(buff, uint32(len(wb.Puts.TheMap)))
	for k, v := range wb.Puts.TheMap {
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

// AddCommittedCallback adds a callback which will be called when the batch is committed
func (wb *WriteBatch) AddCommittedCallback(callback CommittedCallback) {
	wb.committedCallbacks = append(wb.committedCallbacks, callback)
}

// AfterCommit This should be called after committing the batch - it causes any committed callbacks to be run
func (wb *WriteBatch) AfterCommit() error {
	for _, callback := range wb.committedCallbacks {
		if err := callback(); err != nil {
			return err
		}
	}
	return nil
}
