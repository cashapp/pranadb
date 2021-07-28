package dragon

import (
	"bufio"
	"github.com/cockroachdb/pebble"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/table"
	"io"
	"math"
)

var syncWriteOptions = &pebble.WriteOptions{Sync: true}
var nosyncWriteOptions = &pebble.WriteOptions{Sync: false}

func saveSnapshotDataToWriter(snapshot *pebble.Snapshot, prefix []byte, writer io.Writer, shardID uint64) error {
	bufWriter := bufio.NewWriterSize(writer, snapshotSaveBufferSize)
	upper := common.IncrementBytesLittleEndian(prefix)
	iter := snapshot.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: upper})
	for iter.First(); iter.Valid(); iter.Next() {
		k := iter.Key()
		v := iter.Value()
		theShardID := common.ReadUint64FromBufferLittleEndian(k, 8)
		if theShardID != shardID {
			// Sanity check
			panic("wrong shard id!")
		}
		lk := len(k)
		lv := len(v)
		if lk == 0 {
			panic("invalid zero length key")
		}
		if lv == 0 {
			panic("invalid zero length value")
		}
		if lk > math.MaxUint32 {
			panic("key too long")
		}
		if lv > math.MaxUint32 {
			panic("value too long")
		}
		buff := make([]byte, 0, lk+lv+8)
		buff = common.AppendUint32ToBufferLittleEndian(buff, uint32(len(k)))
		buff = append(buff, k...)
		buff = common.AppendUint32ToBufferLittleEndian(buff, uint32(len(v)))
		buff = append(buff, v...)

		if _, err := bufWriter.Write(buff); err != nil {
			return err
		}
	}
	// Write 0 to signify end of stream
	buff := make([]byte, 0, 4)
	buff = common.AppendUint32ToBufferLittleEndian(buff, 0)
	if _, err := bufWriter.Write(buff); err != nil {
		return err
	}
	if err := bufWriter.Flush(); err != nil {
		return err
	}
	return snapshot.Close()
}

func restoreSnapshotDataFromReader(peb *pebble.DB, startPrefix []byte, endPrefix []byte, reader io.Reader) error {

	wo := syncWriteOptions

	batch := peb.NewBatch()

	// Delete the data for the state machine - we're going to replace it
	if err := batch.DeleteRange(startPrefix, endPrefix, &pebble.WriteOptions{}); err != nil {
		return err
	}

	bufReader := bufio.NewReaderSize(reader, snapshotRecoverBufferSize)

	// Load kv pairs into the shard from the stream
	batchSize := 0
	lenBuf := make([]byte, 0, 4)
	for {
		lenBuf = lenBuf[0:4]
		if _, err := io.ReadFull(bufReader, lenBuf); err != nil {
			return err
		}
		lk := common.ReadUint32FromBufferLittleEndian(lenBuf, 0)
		if lk == 0 {
			// Signifies end of stream
			break
		}
		kb := make([]byte, 0, lk)
		if _, err := io.ReadFull(bufReader, kb); err != nil {
			return err
		}

		lenBuf = lenBuf[0:4]
		if _, err := io.ReadFull(bufReader, lenBuf); err != nil {
			return err
		}
		lv := common.ReadUint32FromBufferLittleEndian(lenBuf, 0)
		vb := make([]byte, 0, lv)
		if _, err := io.ReadFull(bufReader, vb); err != nil {
			return err
		}

		if err := batch.Set(kb, vb, nil); err != nil {
			return err
		}
		batchSize++
		if batchSize == maxSnapshotRecoverBatchSize {
			if err := peb.Apply(batch, wo); err != nil {
				return err
			}
			batch = peb.NewBatch()
			batchSize = 0
		}
	}
	return peb.Apply(batch, wo)
}

func syncPebble(peb *pebble.DB) error {
	// To force an fsync we just write a kv into the dummy sys table with sync = true
	prefix := make([]byte, 0, 8)
	common.AppendUint64ToBufferLittleEndian(prefix, common.SyncTableID)
	return peb.Set(prefix, []byte{}, syncWriteOptions)
}

func loadLastProcessedRaftIndex(peb *pebble.DB, shardID uint64) (uint64, error) {
	// read the index of the last persisted log entry
	key := table.EncodeTableKeyPrefix(common.LastLogIndexReceivedTableID, shardID, 16)
	vb, closer, err := peb.Get(key)
	defer common.InvokeCloser(closer)
	if err == pebble.ErrNotFound {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	lastIndex := common.ReadUint64FromBufferLittleEndian(vb, 0)
	return lastIndex, nil
}

func writeLastIndexValue(batch *pebble.Batch, val uint64, shardID uint64) error {
	// We store the last received and persisted log entry
	key := table.EncodeTableKeyPrefix(common.LastLogIndexReceivedTableID, shardID, 16)
	vb := make([]byte, 0, 8)
	vb = common.AppendUint64ToBufferLittleEndian(vb, val)
	return batch.Set(key, vb, nil)
}
