package storage

import (
	"encoding/binary"
	"github.com/google/btree"
	"sync"
)

type FakeStorage struct {
	btree *btree.BTree
	mu    sync.RWMutex
}

func NewFakeStorage() Storage {
	btree := btree.New(3)
	return &FakeStorage{
		btree: btree,
	}
}

func (f *FakeStorage) writeBatch(partitionID uint64, batch *WriteBatch, localLeader bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, kvPair := range batch.puts {
		internalKey := internalKey(partitionID, kvPair.Key)
		f.putInternal(&kvWrapper{
			key:   internalKey,
			value: kvPair.Value,
		})
	}
	for _, key := range batch.deletes {
		internalKey := internalKey(partitionID, key)
		f.deleteInternal(&kvWrapper{key: internalKey})
	}
	return nil
}

func (f *FakeStorage) installExecutors(partitionID uint64, plan *ExecutorPlan) {
	panic("implement me")
}

func (f *FakeStorage) get(partitionID uint64, key []byte) ([]byte, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	internalKey := internalKey(partitionID, key)
	return f.getInternal(&kvWrapper{key: internalKey}), nil
}

// TODO should probably return an iterator
func (f *FakeStorage) scan(partitionID uint64, startKeyPrefix []byte, endKeyPrefix []byte, limit int) ([]KVPair, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if startKeyPrefix == nil {
		panic("startKeyPrefix cannot be nil")
	}
	startPrefix := internalKey(partitionID, startKeyPrefix)
	endPrefix := internalKey(partitionID, endKeyPrefix)
	var result []KVPair
	count := 0
	resFunc := func(i btree.Item) bool {
		wrapper := i.(*kvWrapper)
		result = append(result, KVPair{
			// First 8 bytes is the partition key, we remove this
			Key:   wrapper.key[8:],
			Value: wrapper.value,
		})
		count++
		return limit == -1 || count < limit
	}
	if endKeyPrefix != nil {
		f.btree.AscendRange(&kvWrapper{key: startPrefix}, &kvWrapper{key: endPrefix}, resFunc)
	} else {
		f.btree.AscendGreaterOrEqual(&kvWrapper{key: startPrefix}, resFunc)
	}
	return result, nil
}

type kvWrapper struct {
	key   []byte
	value []byte
}

func (k kvWrapper) Less(than btree.Item) bool {
	otherKVwrapper := than.(*kvWrapper)

	thisKey := k.key
	otherKey := otherKVwrapper.key

	return compareBytes(thisKey, otherKey) < 0
}

func compareBytes(b1 []byte, b2 []byte) int {
	lb1 := len(b1)
	lb2 := len(b2)
	var min int
	if lb1 < lb2 {
		min = lb1
	} else {
		min = lb2
	}
	for i := 0; i < min; i++ {
		byte1 := b1[i]
		byte2 := b2[i]
		if byte1 == byte2 {
			continue
		} else if byte1 > byte2 {
			return 1
		} else {
			return -1
		}
	}
	if lb1 == lb2 {
		return 0
	} else if lb1 > lb2 {
		return 1
	} else {
		return -1
	}
}

func (f *FakeStorage) putInternal(item *kvWrapper) {
	f.btree.ReplaceOrInsert(item)
}

func (f *FakeStorage) deleteInternal(item *kvWrapper) {
	f.btree.ReplaceOrInsert(item)
}

func (f *FakeStorage) getInternal(key *kvWrapper) []byte {
	item := f.btree.Get(key)
	if item != nil {
		wrapper := item.(*kvWrapper)
		return wrapper.value
	} else {
		return nil
	}
}

// Internal key is partitionID bytes concatenated with key bytes
func internalKey(partitionID uint64, key []byte) []byte {
	res := make([]byte, 0, 8+len(key))
	partBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(partBytes, partitionID)
	res = append(res, partBytes...)
	res = append(res, key...)
	return res
}
