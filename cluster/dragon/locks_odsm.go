package dragon

import (
	"fmt"
	"io"
	"math"
	"strings"
	"sync"

	"github.com/cockroachdb/pebble"
	"github.com/lni/dragonboat/v3/statemachine"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/table"
)

const (
	locksStateMachineUpdatedOK uint64 = 1

	GetLockCommand          = "get"
	ReleaseLockCommand      = "release"
	LockSMResultTrue   byte = 1
	LockSMResultFalse  byte = 0
)

func (d *Dragon) newLocksODStateMachine(clusterID uint64, _ uint64) statemachine.IOnDiskStateMachine {
	return &locksODStateMachine{
		dragon:  d,
		shardID: clusterID,
	}
}

type locksODStateMachine struct {
	dragon  *Dragon
	shardID uint64
	// This mutex is to provide a memory barrier between calling threads, not for mutual exclusion
	locksLock sync.Mutex
	locks     map[string]struct{} // TODO use a trie
}

func (s *locksODStateMachine) loadLocks() error {
	s.locks = make(map[string]struct{})
	startPrefix := table.EncodeTableKeyPrefix(common.LocksTableID, s.shardID, 16)
	endPrefix := table.EncodeTableKeyPrefix(common.LocksTableID+1, s.shardID, 16)
	kvp, err := s.dragon.LocalScan(startPrefix, endPrefix, math.MaxInt32)
	if err != nil {
		return err
	}
	for _, kv := range kvp {
		sKey, _ := common.ReadStringFromBufferBE(kv.Key, 16)
		s.locks[sKey] = struct{}{}
	}
	return nil
}

func (s *locksODStateMachine) Open(stopc <-chan struct{}) (uint64, error) {
	s.locksLock.Lock()
	defer s.locksLock.Unlock()
	lp, err := loadLastProcessedRaftIndex(s.dragon.pebble, s.shardID)
	if err != nil {
		return 0, err
	}
	if err := s.loadLocks(); err != nil {
		return 0, err
	}
	return lp, nil
}

func (s *locksODStateMachine) Update(entries []statemachine.Entry) ([]statemachine.Entry, error) {
	s.locksLock.Lock()
	defer s.locksLock.Unlock()
	batch := s.dragon.pebble.NewBatch()
outer:
	for i, entry := range entries {
		offset := 0
		var command string
		command, offset = common.ReadStringFromBufferLE(entry.Cmd, offset)
		prefix, _ := common.ReadStringFromBufferLE(entry.Cmd, offset)
		if command == GetLockCommand {
			for k := range s.locks {
				// If one is a prefix of the other, they can't be held together
				if strings.HasPrefix(k, prefix) || strings.HasPrefix(prefix, k) {
					// Lock already held
					s.setResult(false, entries, i)
					continue outer
				}
			}
			s.locks[prefix] = struct{}{}
			keyBuff := s.encodeLocksKey(prefix)
			if err := batch.Set(keyBuff, nil, &pebble.WriteOptions{Sync: false}); err != nil {
				return nil, err
			}
			s.setResult(true, entries, i)
		} else if command == ReleaseLockCommand {
			_, ok := s.locks[prefix]
			if !ok {
				s.setResult(false, entries, i)
				continue
			}
			delete(s.locks, prefix)
			keyBuff := s.encodeLocksKey(prefix)
			if err := batch.Delete(keyBuff, &pebble.WriteOptions{Sync: false}); err != nil {
				return nil, err
			}
			s.setResult(true, entries, i)
		} else {
			return nil, fmt.Errorf("unknown lock command %s", command)
		}
	}
	if err := writeLastIndexValue(batch, entries[len(entries)-1].Index, s.shardID); err != nil {
		return nil, err
	}
	if err := s.dragon.pebble.Apply(batch, nosyncWriteOptions); err != nil {
		return nil, err
	}
	return entries, nil
}

func (s *locksODStateMachine) setResult(ok bool, entries []statemachine.Entry, i int) {
	entries[i].Result.Value = locksStateMachineUpdatedOK
	if ok {
		entries[i].Result.Data = []byte{LockSMResultTrue}
	} else {
		entries[i].Result.Data = []byte{LockSMResultFalse}
	}
}

func (s *locksODStateMachine) encodeLocksKey(prefix string) []byte {
	keyBuff := table.EncodeTableKeyPrefix(common.LocksTableID, s.shardID, 24)
	return common.KeyEncodeString(keyBuff, prefix)
}

func (s *locksODStateMachine) Lookup(i interface{}) (interface{}, error) {
	panic("should not be called")
}

func (s *locksODStateMachine) Sync() error {
	return syncPebble(s.dragon.pebble)
}

func (s *locksODStateMachine) PrepareSnapshot() (interface{}, error) {
	snapshot := s.dragon.pebble.NewSnapshot()
	return snapshot, nil
}

func (s *locksODStateMachine) SaveSnapshot(i interface{}, writer io.Writer, i2 <-chan struct{}) error {
	snapshot, ok := i.(*pebble.Snapshot)
	if !ok {
		panic("not a snapshot")
	}
	prefix := table.EncodeTableKeyPrefix(common.LocksTableID, s.shardID, 16)
	return saveSnapshotDataToWriter(snapshot, prefix, writer, s.shardID)
}

func (s *locksODStateMachine) RecoverFromSnapshot(reader io.Reader, i <-chan struct{}) error {
	s.locksLock.Lock()
	defer s.locksLock.Unlock()
	startPrefix := table.EncodeTableKeyPrefix(common.LocksTableID, s.shardID, 16)
	endPrefix := table.EncodeTableKeyPrefix(common.LocksTableID+1, s.shardID, 16)
	if err := restoreSnapshotDataFromReader(s.dragon.pebble, startPrefix, endPrefix, reader, s.dragon.ingestDir); err != nil {
		return err
	}
	return s.loadLocks()
}

func (s *locksODStateMachine) Close() error {
	return nil
}
