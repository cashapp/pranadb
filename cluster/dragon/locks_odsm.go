package dragon

import (
	"io"
	"math"
	"strings"
	"sync"

	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/errors"

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

func (d *Dragon) newLocksODStateMachine(_ uint64, _ uint64) statemachine.IOnDiskStateMachine {
	return &locksODStateMachine{dragon: d}
}

type locksODStateMachine struct {
	dragon *Dragon
	// This mutex is to provide a memory barrier between calling threads, not for mutual exclusion
	locksLock sync.Mutex
	locks     map[string]string // TODO use a trie
}

func (s *locksODStateMachine) loadLocks() error {
	s.locks = make(map[string]string)
	startPrefix := table.EncodeTableKeyPrefix(common.LocksTableID, locksClusterID, 16)
	endPrefix := table.EncodeTableKeyPrefix(common.LocksTableID+1, locksClusterID, 16)
	kvp, err := s.dragon.LocalScan(startPrefix, endPrefix, math.MaxInt32)
	if err != nil {
		return errors.WithStack(err)
	}
	for _, kv := range kvp {
		sKey, _ := common.ReadStringFromBufferBE(kv.Key, 16)
		s.locks[sKey] = string(kv.Value)
	}
	return nil
}

func (s *locksODStateMachine) Open(stopc <-chan struct{}) (uint64, error) {
	s.locksLock.Lock()
	defer s.locksLock.Unlock()
	lp, err := loadLastProcessedRaftIndex(s.dragon.pebble, locksClusterID)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	if err := s.loadLocks(); err != nil {
		return 0, errors.WithStack(err)
	}
	return lp, nil
}

func (s *locksODStateMachine) Update(entries []statemachine.Entry) ([]statemachine.Entry, error) {
	// this MUST be idempotent - same entries can be applied more than once in case of retry after timeout
	log.Tracef("locks shard update entries %d", len(entries))
	s.locksLock.Lock()
	defer s.locksLock.Unlock()
	batch := s.dragon.pebble.NewBatch()
outer:
	for i, entry := range entries {
		offset := 0
		var command string
		command, offset = common.ReadStringFromBufferLE(entry.Cmd, offset)
		prefix, offset := common.ReadStringFromBufferLE(entry.Cmd, offset)
		locker, _ := common.ReadStringFromBufferLE(entry.Cmd, offset)
		if command == GetLockCommand {
			for k, currLocker := range s.locks {
				// If one is a prefix of the other, they can't be held together
				if strings.HasPrefix(k, prefix) || strings.HasPrefix(prefix, k) {
					// Lock already held
					if locker == currLocker {
						// Already held by same locker - this is ok and gives us the idempotency we need
						log.Debugf("lock already held by same locker %s %s", prefix, locker)
						s.setResult(true, entries, i)
					} else {
						log.Debugf("lock already held! %s", prefix)
						s.setResult(false, entries, i)
					}
					continue outer
				}
			}
			s.locks[prefix] = locker
			keyBuff := s.encodeLocksKey(prefix)
			if err := batch.Set(keyBuff, []byte(locker), &pebble.WriteOptions{Sync: false}); err != nil {
				return nil, errors.WithStack(err)
			}
			s.setResult(true, entries, i)
		} else if command == ReleaseLockCommand {
			_, ok := s.locks[prefix]
			if !ok {
				// Release lock must always succeed to provide the idempotency guarantee we need
				s.setResult(true, entries, i)
				continue
			}
			delete(s.locks, prefix)
			keyBuff := s.encodeLocksKey(prefix)
			if err := batch.Delete(keyBuff, &pebble.WriteOptions{Sync: false}); err != nil {
				return nil, errors.WithStack(err)
			}
			s.setResult(true, entries, i)
		} else {
			return nil, errors.Errorf("unknown lock command %s", command)
		}
	}
	if err := writeLastIndexValue(batch, entries[len(entries)-1].Index, locksClusterID); err != nil {
		return nil, errors.WithStack(err)
	}
	if err := s.dragon.pebble.Apply(batch, nosyncWriteOptions); err != nil {
		return nil, errors.WithStack(err)
	}
	log.Trace("locks shard updated")
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
	keyBuff := table.EncodeTableKeyPrefix(common.LocksTableID, locksClusterID, 24)
	return common.KeyEncodeString(keyBuff, prefix)
}

func (s *locksODStateMachine) Lookup(i interface{}) (interface{}, error) {
	return nil, nil
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
	prefix := table.EncodeTableKeyPrefix(common.LocksTableID, locksClusterID, 16)
	log.Printf("Saving locks snapshot on node id %d for shard id %d prefix is %v", s.dragon.cnf.NodeID, locksClusterID, prefix)
	return saveSnapshotDataToWriter(snapshot, prefix, writer, locksClusterID)
}

func (s *locksODStateMachine) RecoverFromSnapshot(reader io.Reader, i <-chan struct{}) error {
	log.Info("locks shard recover from snapshot")
	s.locksLock.Lock()
	defer s.locksLock.Unlock()
	startPrefix := table.EncodeTableKeyPrefix(common.LocksTableID, locksClusterID, 16)
	endPrefix := table.EncodeTableKeyPrefix(common.LocksTableID+1, locksClusterID, 16)
	log.Infof("Restoring locks snapshot on node %d", s.dragon.cnf.NodeID)
	if err := restoreSnapshotDataFromReader(s.dragon.pebble, startPrefix, endPrefix, reader, s.dragon.ingestDir); err != nil {
		return errors.WithStack(err)
	}
	err := s.loadLocks()
	log.Info("locks shard recover from snapshot done")
	return err
}

func (s *locksODStateMachine) Close() error {
	return nil
}
