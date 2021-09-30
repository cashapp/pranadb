package dragon

import (
	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/perrors"
	"io"
	"io/ioutil"
	"math"
	"os"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/table"
)

var syncWriteOptions = &pebble.WriteOptions{Sync: true}
var nosyncWriteOptions = &pebble.WriteOptions{Sync: false}

func saveSnapshotDataToWriter(snapshot *pebble.Snapshot, prefix []byte, writer io.Writer, shardID uint64) error {
	var w writeCloseSyncer
	w, ok := writer.(writeCloseSyncer)
	if !ok {
		w = &nopWriteCloseSyncer{writer}
	}
	tbl := sstable.NewWriter(w, sstable.WriterOptions{})
	upper := common.IncrementBytesBigEndian(prefix)
	iter := snapshot.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: upper})
	for iter.First(); iter.Valid(); iter.Next() {
		k := iter.Key()
		v := iter.Value()
		theShardID, _ := common.ReadUint64FromBufferBE(k, 0)
		if theShardID != shardID {
			// Sanity check
			return perrors.Errorf("wrong shard id is %d was expecting %d key is %v upper bound is %v", theShardID, shardID, k, upper)
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
		if err := tbl.Add(sstable.InternalKey{UserKey: k, Trailer: sstable.InternalKeyKindSet}, v); err != nil {
			return err
		}
	}
	if err := tbl.Close(); err != nil {
		return err
	}
	return snapshot.Close()
}

func restoreSnapshotDataFromReader(peb *pebble.DB, startPrefix []byte, endPrefix []byte, reader io.Reader, ingestDir string) error {
	log.Info("Creating temp snapshot recover file")
	f, err := ioutil.TempFile(ingestDir, "")
	if err != nil {
		log.Errorf("Failed to create temp snapshot recover file %+v", err)
		return err
	}
	path := f.Name()
	log.Infof("Created temp snapshot recover file %s", path)
	defer func() {
		// Remove the file if we fail to ingest, to not leave around garbage data filling up the disk.
		// After pebble ingests the file, it will be moved, so we expect this to fail in the good case.
		// Thus ignore any errors.
		log.Infof("In defer func, closing and deleting snapshot recover file %s", path)
		_ = f.Close()
		_ = os.Remove(path)
		log.Infof("In defer func, closed and deleted snapshot recover file %s", path)
	}()

	log.Info("Copying reader to temp file")
	if _, err := io.Copy(f, reader); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		log.Errorf("Failed to sync temp snapshot recover file %+v", err)
		return err
	}
	if err := f.Close(); err != nil {
		log.Errorf("Failed to close temp snapshot recover file %+v", err)
		return err
	}
	log.Info("Copied reader to temp file")

	batch := peb.NewBatch()
	// Delete the data for the state machine - we're going to replace it
	if err := batch.DeleteRange(startPrefix, endPrefix, &pebble.WriteOptions{}); err != nil {
		return err
	}
	log.Info("deleted data for shard, now applying snapshot to pebble")
	if err := peb.Apply(batch, syncWriteOptions); err != nil {
		log.Errorf("Failed to apply delete range %+v", err)
		return err
	}
	log.Info("Applied delete range to pebble, now applying snapshot to pebble")

	err = peb.Ingest([]string{path})

	if err == nil {
		log.Info("successfully applied snapshot")
	} else {
		log.Errorf("failed to apply snapshot to pebble %+v", err)
	}

	return err
}

func syncPebble(peb *pebble.DB) error {
	// To force an fsync we just write a kv into the dummy sys table with sync = true
	key := common.AppendUint64ToBufferLE(make([]byte, 0, 8), common.SyncTableID)
	return peb.Set(key, []byte{}, syncWriteOptions)
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
	lastIndex, _ := common.ReadUint64FromBufferLE(vb, 0)
	return lastIndex, nil
}

func writeLastIndexValue(batch *pebble.Batch, val uint64, shardID uint64) error {
	// We store the last received and persisted log entry
	key := table.EncodeTableKeyPrefix(common.LastLogIndexReceivedTableID, shardID, 16)
	vb := make([]byte, 0, 8)
	vb = common.AppendUint64ToBufferLE(vb, val)
	return batch.Set(key, vb, nil)
}

type writeCloseSyncer interface {
	io.WriteCloser
	Sync() error
}

type nopWriteCloseSyncer struct{ io.Writer }

func (w *nopWriteCloseSyncer) Close() error {
	return nil
}

func (w *nopWriteCloseSyncer) Sync() error {
	return nil
}
