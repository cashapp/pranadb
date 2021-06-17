package storage

import (
	"fmt"
	"github.com/google/btree"
	"sync"
)

type FakeStorage struct {
	btree              *btree.BTree
	mu                 sync.RWMutex
	clusterInfo        *ClusterInfo
	tableSequence      uint64
	remoteWriteHandler RemoteWriteHandler
}

func (f *FakeStorage) SetRemoteWriteHandler(handler RemoteWriteHandler) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.remoteWriteHandler = handler
}

func NewFakeStorage(nodeID int, numShards int) Storage {
	btree := btree.New(3)
	return &FakeStorage{
		btree:       btree,
		clusterInfo: createClusterInfo(nodeID, numShards),
	}
}

func (f *FakeStorage) WriteBatch(batch *WriteBatch, localLeader bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, kvPair := range batch.puts {
		f.putInternal(&kvWrapper{
			key:   kvPair.Key,
			value: kvPair.Value,
		})
	}
	for _, key := range batch.deletes {
		f.deleteInternal(&kvWrapper{key: key})
	}
	if !localLeader && f.remoteWriteHandler != nil {
		f.remoteWriteHandler.RemoteWriteOccurred(batch.ShardID)
	}
	return nil
}

func (f *FakeStorage) InstallExecutors(shardID uint64, plan *ExecutorPlan) {
	panic("implement me")
}

func (f *FakeStorage) GenerateTableID() (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	res := f.tableSequence
	f.tableSequence++
	return res, nil
}

func (f *FakeStorage) Get(shardID uint64, key []byte, localLeader bool) ([]byte, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.getInternal(&kvWrapper{key: key}), nil
}

// TODO should probably return an iterator
func (f *FakeStorage) Scan(shardID uint64, startKeyPrefix []byte, endKeyPrefix []byte, limit int) ([]KVPair, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if startKeyPrefix == nil {
		panic("startKeyPrefix cannot be nil")
	}
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
		f.btree.AscendRange(&kvWrapper{key: startKeyPrefix}, &kvWrapper{key: endKeyPrefix}, resFunc)
	} else {
		f.btree.AscendGreaterOrEqual(&kvWrapper{key: startKeyPrefix}, resFunc)
	}
	return result, nil
}

func (f *FakeStorage) AddShard(shardID uint64, callback ShardCallback) error {
	panic("implement me")
}

func (f *FakeStorage) RemoveShard(shardID uint64) error {
	panic("implement me")
}

func (f *FakeStorage) GetClusterInfo() (*ClusterInfo, error) {
	return f.clusterInfo, nil
}

func (f *FakeStorage) GetNodeInfo(nodeID int) (*NodeInfo, error) {
	nodeInfo, ok := f.clusterInfo.NodeInfos[nodeID]
	if !ok {
		return nil, fmt.Errorf("Invalid node id %d", nodeID)
	}
	return nodeInfo, nil
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

func createClusterInfo(nodeID int, numShards int) *ClusterInfo {
	leaders := make([]uint64, numShards)
	for i := 0; i < numShards; i++ {
		leaders[i] = uint64(i)
	}
	nodeInfo := &NodeInfo{
		Leaders:   leaders,
		Followers: nil,
	}
	nodeInfos := make(map[int]*NodeInfo)
	nodeInfos[nodeID] = nodeInfo
	return &ClusterInfo{NodeInfos: nodeInfos}
}
