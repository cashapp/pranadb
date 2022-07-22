/*
 *  Copyright 2022 Square Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package fake

import (
	"bytes"
	"fmt"
	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/remoting"
	"github.com/squareup/pranadb/table"
	"strings"
	"sync"

	"github.com/google/btree"

	"github.com/squareup/pranadb/common"
)

type FakeCluster struct {
	nodeID                       int
	mu                           sync.RWMutex
	clusterSequence              uint64
	remoteQueryExecutionCallback cluster.RemoteQueryExecutionCallback
	allShardIds                  []uint64
	started                      bool
	btree                        *btree.BTree
	shardListenerFactory         cluster.ShardListenerFactory
	shardListeners               map[uint64]cluster.ShardListener
	lockslock                    sync.Mutex
	locks                        map[string]struct{}
	dedupMaps                    map[uint64]map[string]uint64
	receiverSequences            map[uint64]uint64
}

type snapshot struct {
	btree *btree.BTree
}

func (s snapshot) Close() {
}

func (f *FakeCluster) CreateSnapshot() (cluster.Snapshot, error) {
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
		nodeID:            nodeID,
		clusterSequence:   uint64(common.UserTableIDBase), // First 100 reserved for system tables
		allShardIds:       genAllShardIds(numShards),
		btree:             btree.New(3),
		shardListeners:    make(map[uint64]cluster.ShardListener),
		locks:             make(map[string]struct{}),
		dedupMaps:         make(map[uint64]map[string]uint64),
		receiverSequences: make(map[uint64]uint64),
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

func (f *FakeCluster) ExecuteRemotePullQuery(queryInfo *cluster.QueryExecutionInfo, rowsFactory *common.RowsFactory) (*common.Rows, error) {
	return f.remoteQueryExecutionCallback.ExecuteRemotePullQuery(queryInfo)
}

func (f *FakeCluster) SetRemoteQueryExecutionCallback(callback cluster.RemoteQueryExecutionCallback) {
	f.remoteQueryExecutionCallback = callback
}

func (f *FakeCluster) RegisterShardListenerFactory(factory cluster.ShardListenerFactory) {
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

func (f *FakeCluster) GenerateClusterSequence(sequenceName string) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	res := f.clusterSequence
	f.clusterSequence++
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
	f.shardListeners = make(map[uint64]cluster.ShardListener)
	f.started = false
	return nil
}

func (f *FakeCluster) WriteForwardBatch(batch *cluster.WriteBatch) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	receiverSequence, ok := f.receiverSequences[batch.ShardID]
	if !ok {
		receiverSequence = 0
	}
	filteredBatch := cluster.NewWriteBatch(batch.ShardID)
	dedupMap, ok := f.dedupMaps[batch.ShardID]
	if !ok {
		dedupMap = make(map[string]uint64)
		f.dedupMaps[batch.ShardID] = dedupMap
	}
	var forwardRows []cluster.ForwardRow
	timestamp := common.NanoTime()
	if err := batch.ForEachPut(func(key []byte, value []byte) error {

		dedupKey := key[:24]            // Next 24 bytes is the dedup key
		remoteConsumerBytes := key[24:] // The rest is just the remote consumer id

		ignore, err := cluster.DoDedup(batch.ShardID, dedupKey, dedupMap)
		if err != nil {
			return err
		}
		if ignore {
			return nil
		}

		// We increment before using - receiver sequence must start at 1
		receiverSequence++

		// For a write into the receiver table (forward write) the key is constructed as follows:
		// shard_id|receiver_table_id|batch_sequence|receiver_sequence|remote_consumer_id
		key = table.EncodeTableKeyPrefix(common.ReceiverTableID, batch.ShardID, 40)
		key = common.AppendUint64ToBufferBE(key, receiverSequence)
		key = append(key, remoteConsumerBytes...)

		filteredBatch.AddPut(key, value)

		remoteConsumerID, _ := common.ReadUint64FromBufferBE(remoteConsumerBytes, 0)
		forwardRows = append(forwardRows, cluster.ForwardRow{
			ReceiverSequence: receiverSequence,
			RemoteConsumerID: remoteConsumerID,
			KeyBytes:         key,
			RowBytes:         value,
			WriteTime:        timestamp,
		})

		return nil
	}); err != nil {
		return err
	}
	f.receiverSequences[batch.ShardID] = receiverSequence
	if filteredBatch.HasPuts() {
		if batch.NumDeletes != 0 {
			panic("deletes not supported in forward batch")
		}
		return f.writeBatchInternal(filteredBatch, true, forwardRows)
	}
	return nil
}

func (f *FakeCluster) WriteBatchLocally(batch *cluster.WriteBatch) error {
	return f.WriteBatch(batch)
}

func (f *FakeCluster) WriteBatch(batch *cluster.WriteBatch) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.writeBatchInternal(batch, false, nil); err != nil {
		return err
	}
	return batch.AfterCommit()
}

func (f *FakeCluster) writeBatchInternal(batch *cluster.WriteBatch, forward bool, forwardRows []cluster.ForwardRow) error {
	if batch.ShardID < cluster.DataShardIDBase {
		panic(fmt.Sprintf("invalid shard cluster id %d", batch.ShardID))
	}
	if err := batch.ForEachPut(func(k []byte, v []byte) error {
		f.putInternal(&kvWrapper{
			key:   k,
			value: v,
		})
		return nil
	}); err != nil {
		return err
	}
	if err := batch.ForEachDelete(func(key []byte) error {
		return f.deleteInternal(&kvWrapper{
			key: key,
		})
	}); err != nil {
		return err
	}
	if forward {
		shardListener := f.shardListeners[batch.ShardID]
		shardListener.RemoteWriteOccurred(forwardRows)
	}
	return nil
}

func (f *FakeCluster) LocalGet(key []byte) ([]byte, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.getInternal(&kvWrapper{key: key}), nil
}

func (f *FakeCluster) DeleteAllDataInRangeForShardLocally(shardID uint64, startPrefix []byte, endPrefix []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.deleteAllDataInRangeForShard(shardID, startPrefix, endPrefix)
}

func (f *FakeCluster) deleteAllDataInRangeForShard(shardID uint64, startPrefix []byte, endPrefix []byte) error {
	startPref := make([]byte, 0, 16)
	startPref = common.AppendUint64ToBufferBE(startPref, shardID)
	startPref = append(startPref, startPrefix...)

	endPref := make([]byte, 0, 16)
	endPref = common.AppendUint64ToBufferBE(endPref, shardID)
	endPref = append(endPref, endPrefix...)

	pairs, err := f.localScanWithBtree(f.btree, startPref, endPref, -1)
	if err != nil {
		return errors.WithStack(err)
	}
	for _, pair := range pairs {
		err := f.deleteInternal(&kvWrapper{
			key: pair.Key,
		})
		if err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func (f *FakeCluster) DeleteAllDataInRangeForAllShardsLocally(startPrefix []byte, endPrefix []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, shardID := range f.allShardIds {
		if err := f.deleteAllDataInRangeForShard(shardID, startPrefix, endPrefix); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func (f *FakeCluster) LocalScanWithSnapshot(sn cluster.Snapshot, startKeyPrefix []byte, endKeyPrefix []byte, limit int) ([]cluster.KVPair, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	s, ok := sn.(*snapshot)
	if !ok {
		panic("not a snapshot")
	}
	return f.localScanWithBtree(s.btree, startKeyPrefix, endKeyPrefix, limit)
}

func (f *FakeCluster) LocalScan(startKeyPrefix []byte, endKeyPrefix []byte, limit int) ([]cluster.KVPair, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.localScanWithBtree(f.btree, startKeyPrefix, endKeyPrefix, limit)
}

func (f *FakeCluster) localScanWithBtree(bt *btree.BTree, startKeyPrefix []byte, endKeyPrefix []byte, limit int) ([]cluster.KVPair, error) {
	if startKeyPrefix == nil {
		panic("startKeyPrefix cannot be nil")
	}
	var result []cluster.KVPair
	count := 0
	resFunc := func(i btree.Item) bool {
		wrapper := i.(*kvWrapper) // nolint: forcetypeassert
		if endKeyPrefix != nil && bytes.Compare(wrapper.key, endKeyPrefix) >= 0 {
			return false
		}
		result = append(result, cluster.KVPair{
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
		allShards[i] = uint64(i) + cluster.DataShardIDBase
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
		return errors.Error("didn't find item to delete")
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

func (f *FakeCluster) AddToDeleteBatch(batch *cluster.ToDeleteBatch) error {
	return nil
}

func (f *FakeCluster) RemoveToDeleteBatch(batch *cluster.ToDeleteBatch) error {
	return nil
}

func (f *FakeCluster) PostStartChecks(queryExec common.SimpleQueryExec) error {
	return nil
}

func (f *FakeCluster) AddHealthcheckListener(listener remoting.AvailabilityListener) {
}

func (f *FakeCluster) SyncStore() error {
	return nil
}
