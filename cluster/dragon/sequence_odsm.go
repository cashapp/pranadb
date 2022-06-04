package dragon

import (
	"io"

	"github.com/cockroachdb/pebble"
	"github.com/lni/dragonboat/v3/statemachine"
	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/table"
)

const (
	seqStateMachineUpdatedOK uint64 = 1
)

func (d *Dragon) newSequenceODStateMachine(_ uint64, _ uint64) statemachine.IOnDiskStateMachine {
	return &sequenceODStateMachine{dragon: d}
}

type sequenceODStateMachine struct {
	dragon *Dragon
}

func (s *sequenceODStateMachine) Open(stopc <-chan struct{}) (uint64, error) {
	return loadLastProcessedRaftIndex(s.dragon.pebble, tableSequenceClusterID)
}

func (s *sequenceODStateMachine) Update(entries []statemachine.Entry) ([]statemachine.Entry, error) {
	batch := s.dragon.pebble.NewBatch()
	latestSeqVals := make(map[string][]byte)
	for i, entry := range entries {
		seqName, _ := common.ReadStringFromBufferLE(entry.Cmd, 0)
		keyBuff := table.EncodeTableKeyPrefix(common.SequenceGeneratorTableID, tableSequenceClusterID, 16)
		keyBuff = common.KeyEncodeString(keyBuff, seqName)
		// First look in the local cache - we need to cache locally as the same sequence can be updated more than once
		// in the same update batch - and results wouldn't be applied until we've processed all entries
		v, ok := latestSeqVals[string(keyBuff)]
		if !ok {
			// Look in the KV store
			var err error
			v, err = localGet(s.dragon.pebble, keyBuff)
			if err != nil {
				return nil, errors.WithStack(err)
			}
		}
		var seqVal uint64
		var seqBuff []byte
		if v != nil {
			seqVal, _ = common.ReadUint64FromBufferLE(v, 0)
			seqBuff = v
		} else {
			seqVal = 0
			seqBuff = make([]byte, 8)
		}
		vBuff := common.AppendUint64ToBufferLE(nil, seqVal+1)
		if err := batch.Set(keyBuff, vBuff, nosyncWriteOptions); err != nil {
			return nil, errors.WithStack(err)
		}
		latestSeqVals[string(keyBuff)] = vBuff
		entries[i].Result.Value = seqStateMachineUpdatedOK
		entries[i].Result.Data = seqBuff
	}
	if err := writeLastIndexValue(batch, entries[len(entries)-1].Index, tableSequenceClusterID); err != nil {
		return nil, errors.WithStack(err)
	}
	if err := s.dragon.pebble.Apply(batch, nosyncWriteOptions); err != nil {
		return nil, errors.WithStack(err)
	}
	return entries, nil
}

func (s *sequenceODStateMachine) Lookup(i interface{}) (interface{}, error) {
	return nil, nil
}

func (s *sequenceODStateMachine) Sync() error {
	return syncPebble(s.dragon.pebble)
}

func (s *sequenceODStateMachine) PrepareSnapshot() (interface{}, error) {
	snapshot := s.dragon.pebble.NewSnapshot()
	return snapshot, nil
}

func (s *sequenceODStateMachine) SaveSnapshot(i interface{}, writer io.Writer, i2 <-chan struct{}) error {
	log.Info("sequence shard save snapshot")
	snapshot, ok := i.(*pebble.Snapshot)
	if !ok {
		panic("not a snapshot")
	}
	prefix := table.EncodeTableKeyPrefix(common.SequenceGeneratorTableID, tableSequenceClusterID, 16)
	log.Printf("Saving sequence snapshot on node id %d for shard id %d prefix is %v", s.dragon.cnf.NodeID, tableSequenceClusterID, prefix)
	err := saveSnapshotDataToWriter(snapshot, prefix, writer, tableSequenceClusterID)
	log.Info("sequence shard save snapshot done")
	return err
}

func (s *sequenceODStateMachine) RecoverFromSnapshot(reader io.Reader, i <-chan struct{}) error {
	log.Info("sequence shard receover from snapshot")
	startPrefix := table.EncodeTableKeyPrefix(common.SequenceGeneratorTableID, tableSequenceClusterID, 16)
	endPrefix := table.EncodeTableKeyPrefix(common.SequenceGeneratorTableID+1, tableSequenceClusterID, 16)
	log.Infof("Restoring sequence snapshot on node %d", s.dragon.cnf.NodeID)
	err := restoreSnapshotDataFromReader(s.dragon.pebble, startPrefix, endPrefix, reader, s.dragon.ingestDir)
	log.Info("sequence shard recover from snapshot done")
	return err
}

func (s *sequenceODStateMachine) Close() error {
	return nil
}
