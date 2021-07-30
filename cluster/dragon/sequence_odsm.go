package dragon

import (
	"github.com/cockroachdb/pebble"
	"github.com/lni/dragonboat/v3/statemachine"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/table"
	"io"
)

const (
	seqStateMachineUpdatedOK uint64 = 1
)

func (d *Dragon) newSequenceODStateMachine(clusterID uint64, nodeID uint64) statemachine.IOnDiskStateMachine {
	return &sequenceODStateMachine{
		dragon:  d,
		shardID: clusterID,
	}
}

type sequenceODStateMachine struct {
	dragon  *Dragon
	shardID uint64
}

func (s *sequenceODStateMachine) Open(stopc <-chan struct{}) (uint64, error) {
	return loadLastProcessedRaftIndex(s.dragon.pebble, s.shardID)
}

func (s *sequenceODStateMachine) Update(entries []statemachine.Entry) ([]statemachine.Entry, error) {
	batch := s.dragon.pebble.NewBatch()
	for i, entry := range entries {
		seqName, _ := common.ReadStringFromBuffer(entry.Cmd, 0)
		keyBuff := table.EncodeTableKeyPrefix(common.SequenceGeneratorTableID, s.shardID, 16)
		keyBuff = common.AppendStringToBufferLE(keyBuff, seqName)
		v, err := localGet(s.dragon.pebble, keyBuff)
		if err != nil {
			return nil, err
		}
		var seqVal uint64
		var seqBuff []byte
		if v != nil {
			seqVal, _ = common.ReadUint64FromBufferLE(v, 0)
			seqBuff = v
		} else {
			seqVal = common.UserTableIDBase
			seqBuff = make([]byte, 0)
			seqBuff = common.AppendUint64ToBufferLE(seqBuff, seqVal)
		}
		vBuff := make([]byte, 0, 8)
		vBuff = common.AppendUint64ToBufferLE(vBuff, seqVal+1)
		if err := batch.Set(keyBuff, vBuff, &pebble.WriteOptions{Sync: false}); err != nil {
			return nil, err
		}
		entries[i].Result.Value = seqStateMachineUpdatedOK
		entries[i].Result.Data = seqBuff
	}
	if err := writeLastIndexValue(batch, entries[len(entries)-1].Index, s.shardID); err != nil {
		return nil, err
	}
	if err := s.dragon.pebble.Apply(batch, nosyncWriteOptions); err != nil {
		return nil, err
	}
	return entries, nil
}

func (s *sequenceODStateMachine) Lookup(i interface{}) (interface{}, error) {
	panic("should not be called")
}

func (s *sequenceODStateMachine) Sync() error {
	return syncPebble(s.dragon.pebble)
}

func (s *sequenceODStateMachine) PrepareSnapshot() (interface{}, error) {
	snapshot := s.dragon.pebble.NewSnapshot()
	return snapshot, nil
}

func (s *sequenceODStateMachine) SaveSnapshot(i interface{}, writer io.Writer, i2 <-chan struct{}) error {
	snapshot, ok := i.(*pebble.Snapshot)
	if !ok {
		panic("not a snapshot")
	}
	prefix := table.EncodeTableKeyPrefix(common.SequenceGeneratorTableID, s.shardID, 16)
	return saveSnapshotDataToWriter(snapshot, prefix, writer, s.shardID)
}

func (s *sequenceODStateMachine) RecoverFromSnapshot(reader io.Reader, i <-chan struct{}) error {
	startPrefix := table.EncodeTableKeyPrefix(common.SequenceGeneratorTableID, s.shardID, 16)
	endPrefix := table.EncodeTableKeyPrefix(common.SequenceGeneratorTableID+1, s.shardID, 16)
	return restoreSnapshotDataFromReader(s.dragon.pebble, startPrefix, endPrefix, reader)
}

func (s *sequenceODStateMachine) Close() error {
	return nil
}
