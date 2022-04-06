package cluster

import (
	"github.com/squareup/pranadb/common"
)

// WriteBatch represents some Puts and deletes that will be written atomically by the underlying storage implementation
type WriteBatch struct {
	ShardID            uint64
	Puts               []byte
	Deletes            []byte
	NumPuts            int
	NumDeletes         int
	committedCallbacks []CommittedCallback
}

func NewWriteBatch(shardID uint64) *WriteBatch {
	return &WriteBatch{
		ShardID: shardID,
	}
}

type KVReceiver func([]byte, []byte) error

type KReceiver func([]byte) error

type CommittedCallback func() error

func (wb *WriteBatch) AddPut(k []byte, v []byte) {
	wb.Puts = appendBytesWithLength(wb.Puts, k)
	wb.Puts = appendBytesWithLength(wb.Puts, v)
	wb.NumPuts++
}

func (wb *WriteBatch) AddDelete(k []byte) {
	wb.Deletes = appendBytesWithLength(wb.Deletes, k)
	wb.NumDeletes++
}

func (wb *WriteBatch) HasWrites() bool {
	return len(wb.Puts) > 0 || len(wb.Deletes) > 0
}

func (wb *WriteBatch) Serialize(buff []byte) []byte {
	buff = common.AppendUint32ToBufferLE(buff, uint32(wb.NumPuts))
	buff = append(buff, wb.Puts...)
	buff = common.AppendUint32ToBufferLE(buff, uint32(wb.NumDeletes))
	buff = append(buff, wb.Deletes...)
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

func (wb *WriteBatch) ForEachPut(kvReceiver KVReceiver) error {
	offset := 0
	for offset < len(wb.Puts) {
		lk, _ := common.ReadUint32FromBufferLE(wb.Puts, offset)
		offset += 4
		k := wb.Puts[offset : offset+int(lk)]
		offset += int(lk)
		lv, _ := common.ReadUint32FromBufferLE(wb.Puts, offset)
		offset += 4
		v := wb.Puts[offset : offset+int(lv)]
		offset += int(lv)
		if err := kvReceiver(k, v); err != nil {
			return err
		}
	}
	return nil
}

func (wb *WriteBatch) ForEachDelete(kReceiver KReceiver) error {
	offset := 0
	for offset < len(wb.Deletes) {
		lk, _ := common.ReadUint32FromBufferLE(wb.Deletes, offset)
		offset += 4
		k := wb.Deletes[offset : offset+int(lk)]
		offset += int(lk)
		if err := kReceiver(k); err != nil {
			return err
		}
	}
	return nil
}

func appendBytesWithLength(buff []byte, bytes []byte) []byte {
	buff = common.AppendUint32ToBufferLE(buff, uint32(len(bytes)))
	buff = append(buff, bytes...)
	return buff
}
