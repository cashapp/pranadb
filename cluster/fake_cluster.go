package cluster

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"sync"

	"github.com/google/btree"

	"github.com/squareup/pranadb/common"
)

type fakeCluster struct {
	nodeID                       int
	mu                           sync.RWMutex
	tableSequence                uint64
	remoteQueryExecutionCallback RemoteQueryExecutionCallback
	allShardIds                  []uint64
	started                      bool
	btree                        *btree.BTree
	shardListenerFactory         ShardListenerFactory
	shardListeners               map[uint64]ShardListener
	notifListeners               map[NotificationType]NotificationListener
}

func NewFakeCluster(nodeID int, numShards int) Cluster {
	return &fakeCluster{
		nodeID:         nodeID,
		tableSequence:  uint64(UserTableIDBase), // First 100 reserved for system tables
		allShardIds:    genAllShardIds(numShards),
		btree:          btree.New(3),
		shardListeners: make(map[uint64]ShardListener),
		notifListeners: make(map[NotificationType]NotificationListener),
	}
}

func (f *fakeCluster) BroadcastNotification(notification Notification) error {
	listener := f.lookupNotificationListener(notification)
	listener.HandleNotification(notification)
	return nil
}

func (f *fakeCluster) lookupNotificationListener(notification Notification) NotificationListener {
	f.mu.RLock()
	defer f.mu.RUnlock()
	listener, ok := f.notifListeners[TypeForNotification(notification)]
	if !ok {
		panic(fmt.Sprintf("no notification listener for type %d", TypeForNotification(notification)))
	}
	return listener
}

func (f *fakeCluster) RegisterNotificationListener(notificationType NotificationType, listener NotificationListener) {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.notifListeners[notificationType]
	if ok {
		panic(fmt.Sprintf("notification listener with type %d already registered", notificationType))
	}
	f.notifListeners[notificationType] = listener
}

func (f *fakeCluster) ExecuteRemotePullQuery(schemaName string, query string, queryID string, limit int, shardID uint64, rowsFactory *common.RowsFactory) (*common.Rows, error) {
	log.Printf("Executing remote query on shardID %d", shardID)
	return f.remoteQueryExecutionCallback.ExecuteRemotePullQuery(schemaName, query, queryID, limit, shardID)
}

func (f *fakeCluster) SetRemoteQueryExecutionCallback(callback RemoteQueryExecutionCallback) {
	f.remoteQueryExecutionCallback = callback
}

func (f *fakeCluster) RegisterShardListenerFactory(factory ShardListenerFactory) {
	f.shardListenerFactory = factory
}

func (f *fakeCluster) GetNodeID() int {
	return f.nodeID
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
	log.Printf("Writing batch, puts %d, Deletes %d", len(batch.puts.TheMap), len(batch.Deletes.TheMap))
	for k, v := range batch.puts.TheMap {
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
