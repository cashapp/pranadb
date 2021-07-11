package cluster

import (
	"bytes"
	"errors"
	"github.com/google/btree"
	"github.com/squareup/pranadb/common"
	"log"
	"sync"
)

type fakeCluster struct {
	nodeID                       int
	mu                           sync.RWMutex
	tableSequence                uint64
	remoteQueryExecutionCallback RemoteQueryExecutionCallback
	allShardIds                  []uint64
	allNodes                     []int
	started                      bool
	btree                        *btree.BTree
	shardListenerFactory         ShardListenerFactory
	shardListeners               map[uint64]ShardListener
}

func NewFakeCluster(nodeID int, numShards int) Cluster {
	return &fakeCluster{
		nodeID:         nodeID,
		tableSequence:  uint64(100), // First 100 reserved for system tables
		allShardIds:    genAllShardIds(numShards),
		allNodes:       genAllNodes(1),
		btree:          btree.New(3),
		shardListeners: make(map[uint64]ShardListener),
	}
}

func (f *fakeCluster) ExecuteRemotePullQuery(schemaName string, query string, queryID string, limit int, nodeID int) chan RemoteQueryResult {
	f.mu.Lock()
	callback := f.remoteQueryExecutionCallback
	f.mu.Unlock()
	ch := make(chan RemoteQueryResult, 1)
	if callback != nil {
		go func() {
			rows, err := callback.ExecuteRemotePullQuery(schemaName, query, queryID, limit)
			ch <- RemoteQueryResult{
				Rows: rows,
				Err:  err,
			}
		}()
		return ch
	}
	ch <- RemoteQueryResult{
		Err: errors.New("no remote query callback registered"),
	}
	return ch
}

func (f *fakeCluster) SetRemoteQueryExecutionCallback(callback RemoteQueryExecutionCallback) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.started {
		panic("Cannot set remote query execution callback after cluster is started")
	}
	f.remoteQueryExecutionCallback = callback
}

func (f *fakeCluster) RegisterShardListenerFactory(factory ShardListenerFactory) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.shardListenerFactory = factory
}

func (f *fakeCluster) GetNodeID() int {
	return f.nodeID
}

func (f *fakeCluster) GetAllNodeIDs() []int {
	return f.allNodes
}

func (f *fakeCluster) GetAllShardIDs() []uint64 {
	return f.allShardIds
}

func (f *fakeCluster) GetLocalShardIDs() []uint64 {
	return f.allShardIds
}

func (f *fakeCluster) GenerateTableID() (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	res := f.tableSequence
	f.tableSequence++
	return res, nil
}

func (f *fakeCluster) Start() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.started {
		return nil
	}
	f.startShardListeners()
	f.started = true
	return nil
}

func (f *fakeCluster) Stop() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.started {
		return nil
	}
	f.stopShardListeners()
	f.started = false
	return nil
}

func (f *fakeCluster) WriteBatch(batch *WriteBatch) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	log.Printf("Write batch for shard %d", batch.ShardID)
	log.Printf("Writing batch, Puts %d, Deletes %d", len(batch.Puts.TheMap), len(batch.Deletes.TheMap))
	for k, v := range batch.Puts.TheMap {
		kBytes := common.StringToByteSliceZeroCopy(k)
		log.Printf("Putting key %v value %v", kBytes, v)
		f.putInternal(&kvWrapper{
			key:   kBytes,
			value: v,
		})
	}
	for k := range batch.Deletes.TheMap {
		kBytes := common.StringToByteSliceZeroCopy(k)
		log.Printf("Deleting key %v", kBytes)
		err := f.deleteInternal(&kvWrapper{
			key: kBytes,
		})
		if err != nil {
			return err
		}
	}
	if batch.NotifyRemote {
		shardListener := f.shardListeners[batch.ShardID]
		go shardListener.RemoteWriteOccurred()
	}
	return nil
}

func (f *fakeCluster) LocalGet(key []byte) ([]byte, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.getInternal(&kvWrapper{key: key}), nil
}

func (f *fakeCluster) LocalScan(startKeyPrefix []byte, whileKeyPrefix []byte, limit int) ([]KVPair, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if startKeyPrefix == nil {
		panic("startKeyPrefix cannot be nil")
	}
	if whileKeyPrefix == nil {
		panic("whileKeyPrefix cannot be nil")
	}
	whilePrefixLen := len(whileKeyPrefix)
	var result []KVPair
	count := 0
	resFunc := func(i btree.Item) bool {
		wrapper := i.(*kvWrapper) // nolint: forcetypeassert
		if bytes.Compare(wrapper.key[0:whilePrefixLen], whileKeyPrefix) > 0 {
			return false
		}
		result = append(result, KVPair{
			Key:   wrapper.key,
			Value: wrapper.value,
		})
		count++
		return limit == -1 || count < limit
	}
	f.btree.AscendGreaterOrEqual(&kvWrapper{key: startKeyPrefix}, resFunc)
	return result, nil
}

func (f *fakeCluster) startShardListeners() {
	if f.shardListenerFactory == nil {
		return
	}
	for _, shardID := range f.allShardIds {
		shardListener := f.shardListenerFactory.CreateShardListener(shardID)
		f.shardListeners[shardID] = shardListener
	}
}

func (f *fakeCluster) stopShardListeners() {
	for _, shardListener := range f.shardListeners {
		shardListener.Close()
	}
}

func genAllShardIds(numShards int) []uint64 {
	allShards := make([]uint64, numShards)
	for i := 0; i < numShards; i++ {
		allShards[i] = uint64(i)
	}
	return allShards
}

func genAllNodes(numNodes int) []int {
	allNodes := make([]int, numNodes)
	for i := 0; i < numNodes; i++ {
		allNodes[i] = i
	}
	return allNodes
}

type kvWrapper struct {
	key   []byte
	value []byte
}

func (k kvWrapper) Less(than btree.Item) bool {
	otherKVwrapper := than.(*kvWrapper) // nolint: forcetypeassert

	thisKey := k.key
	otherKey := otherKVwrapper.key

	return bytes.Compare(thisKey, otherKey) < 0
}

func (f *fakeCluster) putInternal(item *kvWrapper) {
	f.btree.ReplaceOrInsert(item)
}

func (f *fakeCluster) deleteInternal(item *kvWrapper) error {
	prevItem := f.btree.Delete(item)
	if prevItem == nil {
		return errors.New("didn't find item to delete")
	}
	return nil
}

func (f *fakeCluster) getInternal(key *kvWrapper) []byte {
	if item := f.btree.Get(key); item != nil {
		wrapper := item.(*kvWrapper) // nolint: forcetypeassert
		return wrapper.value
	}
	return nil
}
