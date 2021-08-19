package cluster

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/google/btree"

	"github.com/squareup/pranadb/common"
)

type FakeCluster struct {
	nodeID                       int
	mu                           sync.RWMutex
	tableSequence                uint64
	remoteQueryExecutionCallback RemoteQueryExecutionCallback
	allShardIds                  []uint64
	started                      bool
	btree                        *btree.BTree
	shardListenerFactory         ShardListenerFactory
	shardListeners               map[uint64]ShardListener
	membershipListener           MembershipListener
	lockslock                    sync.Mutex
	locks                        map[string]struct{} // TODO use a trie
}

type snapshot struct {
	btree *btree.BTree
}

func (s snapshot) Close() {
}

func (f *FakeCluster) CreateSnapshot() (Snapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cloned := f.btree.Clone()
	return &snapshot{btree: cloned}, nil
}

func (f *FakeCluster) GetLock(prefix string) (bool, error) {
	f.lockslock.Lock()
	defer f.lockslock.Unlock()
	for k := range f.locks {
		if strings.HasPrefix(k, prefix) || strings.HasPrefix(prefix, k) {
			return false, nil
		}
	}
	f.locks[prefix] = struct{}{}
	return true, nil
}

func (f *FakeCluster) ReleaseLock(prefix string) (bool, error) {
	f.lockslock.Lock()
	defer f.lockslock.Unlock()
	_, ok := f.locks[prefix]
	if !ok {
		return false, nil
	}
	delete(f.locks, prefix)
	return true, nil
}

func NewFakeCluster(nodeID int, numShards int) *FakeCluster {
	return &FakeCluster{
		nodeID:         nodeID,
		tableSequence:  uint64(common.UserTableIDBase), // First 100 reserved for system tables
		allShardIds:    genAllShardIds(numShards),
		btree:          btree.New(3),
		shardListeners: make(map[uint64]ShardListener),
		locks:          make(map[string]struct{}),
	}
}

func (f *FakeCluster) Dump() string {
	builder := &strings.Builder{}
	builder.WriteString("Dumping database\n")
	f.btree.AscendGreaterOrEqual(&kvWrapper{
		key:   nil,
		value: nil,
	}, func(i btree.Item) bool {
		kv, ok := i.(*kvWrapper)
		if !ok {
			panic("not a kv wrapper")
		}
		s := fmt.Sprintf("key:%s value:%v", common.DumpDataKey(kv.key), kv.value)
		builder.WriteString(s + "\n")
		return true
	})
	return builder.String()
}

func (f *FakeCluster) RegisterMembershipListener(listener MembershipListener) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.membershipListener != nil {
		panic("membership listener already registered")
	}
	f.membershipListener = listener
}

func (f *FakeCluster) ExecuteRemotePullQuery(queryInfo *QueryExecutionInfo, rowsFactory *common.RowsFactory) (*common.Rows, error) {
	return f.remoteQueryExecutionCallback.ExecuteRemotePullQuery(queryInfo)
}

func (f *FakeCluster) SetRemoteQueryExecutionCallback(callback RemoteQueryExecutionCallback) {
	f.remoteQueryExecutionCallback = callback
}

func (f *FakeCluster) RegisterShardListenerFactory(factory ShardListenerFactory) {
	f.shardListenerFactory = factory
}

func (f *FakeCluster) GetNodeID() int {
	return f.nodeID
}

func (f *FakeCluster) GetAllShardIDs() []uint64 {
	return f.allShardIds
}

func (f *FakeCluster) GetLocalShardIDs() []uint64 {
	return f.allShardIds
}

func (f *FakeCluster) GenerateTableID() (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	res := f.tableSequence
	f.tableSequence++
	return res, nil
}

func (f *FakeCluster) Start() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.remoteQueryExecutionCallback == nil {
		panic("remote query execution callback must be set before start")
	}
	if f.shardListenerFactory == nil {
		panic("shard listener factory must be set before start")
	}
	if f.started {
		return nil
	}
	f.startShardListeners()
	f.started = true
	return nil
}

// Stop resets all ephemeral state for a cluster, allowing it to be used with a new
// server but keeping all persisted data.
func (f *FakeCluster) Stop() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.started {
		return nil
	}
	f.shardListeners = make(map[uint64]ShardListener)
	f.started = false
	return nil
}

func (f *FakeCluster) WriteBatch(batch *WriteBatch) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if batch.ShardID < DataShardIDBase {
		panic(fmt.Sprintf("invalid shard cluster id %d", batch.ShardID))
	}
	for k, v := range batch.puts.TheMap {
		kBytes := common.StringToByteSliceZeroCopy(k)
		f.putInternal(&kvWrapper{
			key:   kBytes,
			value: v,
		})
	}
	for k := range batch.Deletes.TheMap {
		kBytes := common.StringToByteSliceZeroCopy(k)
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

func (f *FakeCluster) LocalGet(key []byte) ([]byte, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.getInternal(&kvWrapper{key: key}), nil
}

func (f *FakeCluster) DeleteAllDataInRangeForShard(shardID uint64, startPrefix []byte, endPrefix []byte) error {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.deleteAllDataInRangeForShard(shardID, startPrefix, endPrefix)
}

func (f *FakeCluster) deleteAllDataInRangeForShard(shardID uint64, startPrefix []byte, endPrefix []byte) error {
	startPref := make([]byte, 0, 16)
	startPref = common.AppendUint64ToBufferBE(startPref, shardID)
	startPref = append(startPref, startPrefix...)

	endPref := make([]byte, 0, 16)
	endPref = common.AppendUint64ToBufferBE(endPref, shardID)
	endPref = append(endPref, endPrefix...)

	pairs, err := f.LocalScan(startPref, endPref, -1)
	if err != nil {
		return err
	}
	for _, pair := range pairs {
		err := f.deleteInternal(&kvWrapper{
			key: pair.Key,
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (f *FakeCluster) DeleteAllDataInRangeForAllShards(startPrefix []byte, endPrefix []byte) error {
	f.mu.RLock()
	defer f.mu.RUnlock()
	for _, shardID := range f.allShardIds {
		if err := f.deleteAllDataInRangeForShard(shardID, startPrefix, endPrefix); err != nil {
			return err
		}
	}
	return nil
}

func (f *FakeCluster) LocalScanWithSnapshot(sn Snapshot, startKeyPrefix []byte, endKeyPrefix []byte, limit int) ([]KVPair, error) {
	s, ok := sn.(*snapshot)
	if !ok {
		panic("not a snapshot")
	}
	return f.localScanWithBtree(s.btree, startKeyPrefix, endKeyPrefix, limit)
}

func (f *FakeCluster) LocalScan(startKeyPrefix []byte, endKeyPrefix []byte, limit int) ([]KVPair, error) {
	return f.localScanWithBtree(f.btree, startKeyPrefix, endKeyPrefix, limit)
}

func (f *FakeCluster) localScanWithBtree(bt *btree.BTree, startKeyPrefix []byte, endKeyPrefix []byte, limit int) ([]KVPair, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if startKeyPrefix == nil {
		panic("startKeyPrefix cannot be nil")
	}
	var result []KVPair
	count := 0
	resFunc := func(i btree.Item) bool {
		wrapper := i.(*kvWrapper) // nolint: forcetypeassert
		if endKeyPrefix != nil && bytes.Compare(wrapper.key, endKeyPrefix) >= 0 {
			return false
		}
		result = append(result, KVPair{
			Key:   wrapper.key,
			Value: wrapper.value,
		})
		count++
		return limit == -1 || count < limit
	}
	bt.AscendGreaterOrEqual(&kvWrapper{key: startKeyPrefix}, resFunc)
	return result, nil
}

func (f *FakeCluster) startShardListeners() {
	if f.shardListenerFactory == nil {
		return
	}
	for _, shardID := range f.allShardIds {
		shardListener := f.shardListenerFactory.CreateShardListener(shardID)
		f.shardListeners[shardID] = shardListener
	}
}

func genAllShardIds(numShards int) []uint64 {
	allShards := make([]uint64, numShards)
	for i := 0; i < numShards; i++ {
		allShards[i] = uint64(i) + DataShardIDBase
	}
	return allShards
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

func (f *FakeCluster) putInternal(item *kvWrapper) {
	f.btree.ReplaceOrInsert(item)
}

func (f *FakeCluster) deleteInternal(item *kvWrapper) error {
	prevItem := f.btree.Delete(item)
	if prevItem == nil {
		return errors.New("didn't find item to delete")
	}
	return nil
}

func (f *FakeCluster) getInternal(key *kvWrapper) []byte {
	if item := f.btree.Get(key); item != nil {
		wrapper := item.(*kvWrapper) // nolint: forcetypeassert
		return wrapper.value
	}
	return nil
}
