package dragon

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/lni/dragonboat/v3/logger"
	"github.com/squareup/pranadb/table"

	"github.com/lni/dragonboat/v3/client"
	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/conf"
	"github.com/squareup/pranadb/errors"

	"github.com/cockroachdb/pebble"
	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/config"
	"github.com/lni/dragonboat/v3/raftio"
	"github.com/lni/dragonboat/v3/statemachine"
	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/common"
)

const (
	// The dragon cluster id for the table sequence group
	tableSequenceClusterID uint64 = 1

	locksClusterID uint64 = 2

	sequenceGroupSize = 3

	locksGroupSize = 3

	retryDelay = 500 * time.Millisecond

	retryTimeout = 10 * time.Minute

	callTimeout = 5 * time.Second

	toDeleteShardID uint64 = 4
)

func NewDragon(cnf conf.Config) (*Dragon, error) {
	if len(cnf.RaftAddresses) < 3 {
		return nil, errors.Error("minimum cluster size is 3 nodes")
	}
	return &Dragon{cnf: cnf, shardSMs: make(map[uint64]struct{})}, nil
}

type Dragon struct {
	lock                         sync.RWMutex
	cnf                          conf.Config
	ingestDir                    string
	pebble                       *pebble.DB
	nh                           *dragonboat.NodeHost
	shardAllocs                  map[uint64][]int
	allShards                    []uint64
	localShards                  []uint64
	started                      bool
	shardListenerFactory         cluster.ShardListenerFactory
	remoteQueryExecutionCallback cluster.RemoteQueryExecutionCallback
	membershipListener           cluster.MembershipListener
	shardSMsLock                 sync.Mutex
	shardSMs                     map[uint64]struct{}
}

type snapshot struct {
	pebbleSnapshot *pebble.Snapshot
}

func (s *snapshot) Close() {
	if err := s.pebbleSnapshot.Close(); err != nil {
		log.Errorf("failed to close snapshot %+v", err)
	}
}

func (d *Dragon) RegisterMembershipListener(listener cluster.MembershipListener) {
	d.lock.Lock()
	defer d.lock.Unlock()
	if d.membershipListener != nil {
		panic("membership listener already registered")
	}
	d.membershipListener = listener
}

func (d *Dragon) RegisterShardListenerFactory(factory cluster.ShardListenerFactory) {
	d.shardListenerFactory = factory
}

func (d *Dragon) GetNodeID() int {
	d.lock.Lock()
	defer d.lock.Unlock()
	return d.cnf.NodeID
}

func (d *Dragon) GenerateClusterSequence(sequenceName string) (uint64, error) {
	cs := d.nh.GetNoOPSession(tableSequenceClusterID)

	var buff []byte
	buff = common.AppendStringToBufferLE(buff, sequenceName)

	proposeRes, err := d.proposeWithRetry(cs, buff)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	if proposeRes.Value != seqStateMachineUpdatedOK {
		return 0, errors.Errorf("unexpected return value from writing sequence: %d", proposeRes.Value)
	}
	seqBuff := proposeRes.Data
	seqVal, _ := common.ReadUint64FromBufferLE(seqBuff, 0)
	return seqVal, nil
}

func (d *Dragon) GetLock(prefix string) (bool, error) {
	return d.sendLockRequest(GetLockCommand, prefix)
}

func (d *Dragon) ReleaseLock(prefix string) (bool, error) {
	return d.sendLockRequest(ReleaseLockCommand, prefix)
}

func (d *Dragon) sendLockRequest(command string, prefix string) (bool, error) {
	cs := d.nh.GetNoOPSession(locksClusterID)

	var buff []byte
	buff = common.AppendStringToBufferLE(buff, command)
	buff = common.AppendStringToBufferLE(buff, prefix)

	proposeRes, err := d.proposeWithRetry(cs, buff)
	if err != nil {
		return false, errors.WithStack(err)
	}
	if proposeRes.Value != locksStateMachineUpdatedOK {
		return false, errors.Errorf("unexpected return value from lock request: %d", proposeRes.Value)
	}
	resBuff := proposeRes.Data
	var bRes bool
	if res := resBuff[0]; res == LockSMResultTrue {
		bRes = true
	} else if res == LockSMResultFalse {
		bRes = false
	} else {
		return false, errors.Errorf("unexpected return value from lock request %d", res)
	}
	return bRes, nil
}

func (d *Dragon) GetAllShardIDs() []uint64 {
	d.lock.Lock()
	defer d.lock.Unlock()
	return d.allShards
}

func (d *Dragon) GetLocalShardIDs() []uint64 {
	d.lock.Lock()
	defer d.lock.Unlock()
	return d.localShards
}

// ExecuteRemotePullQuery For now we are executing pull queries through raft. however going ahead we should probably fanout ourselves
// rather than going through raft as going through raft will prevent any writes in same shard at the same time
// and we don't need linearizability for pull queries
func (d *Dragon) ExecuteRemotePullQuery(queryInfo *cluster.QueryExecutionInfo, rowsFactory *common.RowsFactory) (*common.Rows, error) {

	if queryInfo.ShardID < cluster.DataShardIDBase {
		panic("invalid shard cluster id")
	}

	var buff []byte
	buff = append(buff, shardStateMachineLookupQuery)
	queryRequest, err := queryInfo.Serialize(buff)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	res, err := d.executeWithRetry(func() (interface{}, error) {
		ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
		res, err := d.nh.SyncRead(ctx, queryInfo.ShardID, queryRequest)
		cancel()
		return res, errors.WithStack(err)
	}, retryTimeout)

	if err != nil {
		err = errors.WithStack(errors.Errorf("failed to execute query on node %d %s %v", d.cnf.NodeID, queryInfo.Query, err))
		return nil, errors.WithStack(err)
	}
	bytes, ok := res.([]byte)
	if !ok {
		panic("expected []byte")
	}
	if bytes[0] == 0 {
		msg := string(bytes[1:])
		return nil, errors.Errorf("failed to execute remote query %s %v", queryInfo.Query, msg)
	}
	rows := rowsFactory.NewRows(1)
	rows.Deserialize(bytes[1:])
	return rows, nil
}

func (d *Dragon) SetRemoteQueryExecutionCallback(callback cluster.RemoteQueryExecutionCallback) {
	d.remoteQueryExecutionCallback = callback
}

func (d *Dragon) Start() error { // nolint:gocyclo
	d.lock.Lock()
	defer d.lock.Unlock()
	if d.started {
		return nil
	}

	// Dragon logs a lot of non error stuff at error or warn - we screen these out (in tests mainly)
	if d.cnf.ScreenDragonLogSpam {
		logger.GetLogger("dragonboat").SetLevel(logger.ERROR)
		logger.GetLogger("raft").SetLevel(logger.ERROR)
		logger.GetLogger("rsm").SetLevel(logger.ERROR)
		logger.GetLogger("transport").SetLevel(logger.CRITICAL)
		logger.GetLogger("grpc").SetLevel(logger.ERROR)
	} else {
		logger.GetLogger("dragonboat").SetLevel(logger.WARNING)
		logger.GetLogger("raft").SetLevel(logger.WARNING)
		logger.GetLogger("rsm").SetLevel(logger.WARNING)
		logger.GetLogger("transport").SetLevel(logger.WARNING)
		logger.GetLogger("grpc").SetLevel(logger.WARNING)
	}

	log.Debugf("Starting dragon on node %d", d.cnf.NodeID)

	if d.remoteQueryExecutionCallback == nil {
		panic("remote query execution callback must be set before start")
	}
	if d.shardListenerFactory == nil {
		panic("shard listener factory must be set before start")
	}

	datadir := filepath.Join(d.cnf.DataDir, fmt.Sprintf("node-%d", d.cnf.NodeID))
	pebbleDir := filepath.Join(datadir, "pebble")
	d.ingestDir = filepath.Join(datadir, "ingest-snapshots")
	if err := os.MkdirAll(d.ingestDir, 0o750); err != nil {
		return errors.WithStack(err)
	}

	// TODO used tuned config for Pebble - this can be copied from the Dragonboat Pebble config (see kv_pebble.go in Dragonboat)
	pebbleOptions := &pebble.Options{}
	peb, err := pebble.Open(pebbleDir, pebbleOptions)
	if err != nil {
		return errors.WithStack(err)
	}
	d.pebble = peb

	log.Debugf("Opened pebble on node %d", d.cnf.NodeID)

	if err := d.checkConstantShards(d.cnf.NumShards); err != nil {
		return err
	}

	d.generateNodesAndShards(d.cnf.NumShards, d.cnf.ReplicationFactor)

	nodeAddress := d.cnf.RaftAddresses[d.cnf.NodeID]

	dragonBoatDir := filepath.Join(datadir, "dragon")

	nhc := config.NodeHostConfig{
		DeploymentID:        d.cnf.ClusterID,
		WALDir:              dragonBoatDir,
		NodeHostDir:         dragonBoatDir,
		RTTMillisecond:      100,
		RaftAddress:         nodeAddress,
		SystemEventListener: d,
		EnableMetrics:       d.cnf.EnableMetrics,
	}

	nh, err := dragonboat.NewNodeHost(nhc)
	if err != nil {
		return errors.WithStack(err)
	}
	d.nh = nh

	log.Debugf("Joining groups on node %d", d.cnf.NodeID)

	err = d.joinSequenceGroup()
	if err != nil {
		return errors.WithStack(err)
	}

	log.Debugf("Joined sequence group on node %d", d.cnf.NodeID)

	err = d.joinLockGroup()
	if err != nil {
		return errors.WithStack(err)
	}

	log.Debugf("Joined lock group on node %d", d.cnf.NodeID)

	err = d.joinShardGroups()
	if err != nil {
		return errors.WithStack(err)
	}

	log.Debugf("Joined shard groups on node %d", d.cnf.NodeID)

	log.Debugf("Joined node started group on node %d", d.cnf.NodeID)

	// Now we make sure all groups are ready by executing lookups against them.

	log.Infof("Prana node %d waiting for cluster to start", d.cnf.NodeID)

	log.Debug("Checking all data shards are alive")
	req := []byte{shardStateMachineLookupPing}
	for _, shardID := range d.allShards {
		if err := d.ExecutePingLookup(shardID, req); err != nil {
			return errors.WithStack(err)
		}
	}
	log.Debug("Checking lock shard is alive")
	if err := d.ExecutePingLookup(locksClusterID, nil); err != nil {
		return errors.WithStack(err)
	}
	log.Debug("Checking sequence shard is alive")
	if err := d.ExecutePingLookup(tableSequenceClusterID, nil); err != nil {
		return errors.WithStack(err)
	}

	d.started = true

	log.Infof("Prana node %d cluster started", d.cnf.NodeID)

	return nil
}

func (d *Dragon) ExecutePingLookup(shardID uint64, request []byte) error {
	_, err := d.executeWithRetry(func() (interface{}, error) {
		ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
		res, err := d.nh.SyncRead(ctx, shardID, request)
		cancel()
		return res, errors.WithStack(err)
	}, retryTimeout)
	return errors.WithStack(err)
}

func (d *Dragon) Stop() error {
	d.lock.Lock()
	defer d.lock.Unlock()
	if !d.started {
		return nil
	}
	d.nh.Stop()
	err := d.pebble.Close()
	if err == nil {
		d.started = false
	}
	return errors.WithStack(err)
}

func (d *Dragon) WriteBatchLocally(batch *cluster.WriteBatch) error {
	if batch.ShardID < cluster.DataShardIDBase {
		panic(fmt.Sprintf("invalid shard cluster id %d", batch.ShardID))
	}
	pebBatch := d.pebble.NewBatch()
	if err := batch.ForEachPut(func(k []byte, v []byte) error {
		if err := pebBatch.Set(k, v, nil); err != nil {
			return errors.WithStack(err)
		}
		return nil
	}); err != nil {
		return err
	}
	if err := batch.ForEachDelete(func(k []byte) error {
		if err := pebBatch.Delete(k, nil); err != nil {
			return errors.WithStack(err)
		}
		return nil
	}); err != nil {
		return err
	}
	if err := errors.WithStack(d.pebble.Apply(pebBatch, nosyncWriteOptions)); err != nil {
		return err
	}
	return batch.AfterCommit()
}

func (d *Dragon) WriteBatchWithDedup(batch *cluster.WriteBatch) error {
	return d.writeBatchInternal(batch, true)
}

func (d *Dragon) WriteBatch(batch *cluster.WriteBatch) error {
	return d.writeBatchInternal(batch, false)
}

func (d *Dragon) writeBatchInternal(batch *cluster.WriteBatch, dedup bool) error {
	if batch.ShardID < cluster.DataShardIDBase {
		panic(fmt.Sprintf("invalid shard cluster id %d", batch.ShardID))
	}

	cs := d.nh.GetNoOPSession(batch.ShardID)

	var buff []byte
	if dedup {
		buff = append(buff, shardStateMachineCommandForwardWriteWithDedup)
	} else if batch.NotifyRemote {
		buff = append(buff, shardStateMachineCommandForwardWrite)
	} else {
		buff = append(buff, shardStateMachineCommandWrite)
	}
	buff = batch.Serialize(buff)

	proposeRes, err := d.proposeWithRetry(cs, buff)
	if err != nil {
		return errors.WithStack(err)
	}
	if proposeRes.Value != shardStateMachineResponseOK {
		return errors.Errorf("unexpected return value from writing batch: %d to shard %d %d", proposeRes.Value, batch.ShardID, proposeRes.Value)
	}

	return batch.AfterCommit()
}

func (d *Dragon) LocalGet(key []byte) ([]byte, error) {
	return localGet(d.pebble, key)
}

func (d *Dragon) CreateSnapshot() (cluster.Snapshot, error) {
	snap := d.pebble.NewSnapshot()
	return &snapshot{pebbleSnapshot: snap}, nil
}

func (d *Dragon) LocalScanWithSnapshot(sn cluster.Snapshot, startKeyPrefix []byte, endKeyPrefix []byte, limit int) ([]cluster.KVPair, error) {
	if startKeyPrefix == nil {
		panic("startKeyPrefix cannot be nil")
	}
	snap, ok := sn.(*snapshot)
	if !ok {
		panic("not a snapshot")
	}
	iterOptions := &pebble.IterOptions{LowerBound: startKeyPrefix, UpperBound: endKeyPrefix}
	iter := snap.pebbleSnapshot.NewIter(iterOptions)
	pairs, err := d.scanWithIter(iter, startKeyPrefix, limit)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return pairs, nil
}

func (d *Dragon) LocalScan(startKeyPrefix []byte, endKeyPrefix []byte, limit int) ([]cluster.KVPair, error) {
	if startKeyPrefix == nil {
		panic("startKeyPrefix cannot be nil")
	}
	iterOptions := &pebble.IterOptions{LowerBound: startKeyPrefix, UpperBound: endKeyPrefix}
	iter := d.pebble.NewIter(iterOptions)
	return d.scanWithIter(iter, startKeyPrefix, limit)
}

func (d *Dragon) scanWithIter(iter *pebble.Iterator, startKeyPrefix []byte, limit int) ([]cluster.KVPair, error) {
	iter.SeekGE(startKeyPrefix)
	count := 0
	var pairs []cluster.KVPair
	if iter.Valid() {
		for limit == -1 || count < limit {
			k := iter.Key()
			v := iter.Value()
			pairs = append(pairs, cluster.KVPair{
				Key:   common.CopyByteSlice(k), // Must be copied as Pebble reuses the slices
				Value: common.CopyByteSlice(v),
			})
			count++
			if !iter.Next() {
				break
			}
		}
	}
	if err := iter.Close(); err != nil {
		return nil, errors.WithStack(err)
	}
	return pairs, nil
}

func (d *Dragon) DeleteAllDataInRangeForShardLocally(theShardID uint64, startPrefix []byte, endPrefix []byte) error {
	// Remember, key must be in big-endian order
	startPrefixWithShard := make([]byte, 0, 16)
	startPrefixWithShard = common.AppendUint64ToBufferBE(startPrefixWithShard, theShardID)
	startPrefixWithShard = append(startPrefixWithShard, startPrefix...)

	endPrefixWithShard := make([]byte, 0, 16)
	endPrefixWithShard = common.AppendUint64ToBufferBE(endPrefixWithShard, theShardID)
	endPrefixWithShard = append(endPrefixWithShard, endPrefix...)
	batch := d.pebble.NewBatch()
	if err := d.deleteAllDataInRangeLocally(batch, startPrefixWithShard, endPrefixWithShard); err != nil {
		return errors.WithStack(err)
	}
	if err := d.pebble.Apply(batch, nosyncWriteOptions); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (d *Dragon) deleteAllDataInRangeLocally(batch *pebble.Batch, startPrefix []byte, endPrefix []byte) error {
	if err := batch.DeleteRange(startPrefix, endPrefix, nosyncWriteOptions); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (d *Dragon) deleteAllDataInRangeForShard(theShardID uint64, startPrefix []byte, endPrefix []byte) error {
	// Remember, key must be in big-endian order
	startPrefixWithShard := make([]byte, 0, 16)
	startPrefixWithShard = common.AppendUint64ToBufferBE(startPrefixWithShard, theShardID)
	startPrefixWithShard = append(startPrefixWithShard, startPrefix...)

	endPrefixWithShard := make([]byte, 0, 16)
	endPrefixWithShard = common.AppendUint64ToBufferBE(endPrefixWithShard, theShardID)
	endPrefixWithShard = append(endPrefixWithShard, endPrefix...)

	return d.deleteAllDataInRangeForShardFullPrefix(theShardID, startPrefixWithShard, endPrefixWithShard)
}

func (d *Dragon) deleteAllDataInRangeForShardFullPrefix(theShardID uint64, startPrefix []byte, endPrefix []byte) error {
	cs := d.nh.GetNoOPSession(theShardID)

	var buff []byte
	buff = append(buff, shardStateMachineCommandDeleteRangePrefix)

	buff = common.AppendUint32ToBufferLE(buff, uint32(len(startPrefix)))
	buff = append(buff, startPrefix...)
	buff = common.AppendUint32ToBufferLE(buff, uint32(len(endPrefix)))
	buff = append(buff, endPrefix...)

	proposeRes, err := d.proposeWithRetry(cs, buff)
	if err != nil {
		return errors.WithStack(err)
	}
	if proposeRes.Value != shardStateMachineResponseOK {
		return errors.Errorf("unexpected return value %d from request to delete range to shard %d", proposeRes.Value, theShardID)
	}
	return nil
}

func (d *Dragon) DeleteAllDataInRangeForAllShards(startPrefix []byte, endPrefix []byte) error {

	chans := make([]chan error, len(d.allShards))
	for i, shardID := range d.allShards {
		ch := make(chan error, 1)
		chans[i] = ch
		theShardID := shardID
		go func() {
			err := d.deleteAllDataInRangeForShard(theShardID, startPrefix, endPrefix)
			ch <- err
		}()
	}

	for _, ch := range chans {
		err, ok := <-ch
		if !ok {
			panic("channel closed")
		}
		if err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func localGet(peb *pebble.DB, key []byte) ([]byte, error) {
	v, closer, err := peb.Get(key)
	defer common.InvokeCloser(closer)
	if err == pebble.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, errors.WithStack(err)
	}
	res := common.CopyByteSlice(v)
	return res, nil
}

func (d *Dragon) joinShardGroups() error {
	chans := make([]chan error, len(d.shardAllocs))
	index := 0
	for shardID, nodeIDs := range d.shardAllocs {
		ch := make(chan error)
		// We join them in parallel - not that it makes much difference to time
		go d.joinShardGroup(shardID, nodeIDs, ch)
		chans[index] = ch
		index++
	}
	for _, ch := range chans {
		err, ok := <-ch
		if !ok {
			return errors.Error("channel was closed")
		}
		if err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func (d *Dragon) joinShardGroup(shardID uint64, nodeIDs []int, ch chan error) {
	rc := config.Config{
		NodeID:             uint64(d.cnf.NodeID + 1),
		ElectionRTT:        20,
		HeartbeatRTT:       2,
		CheckQuorum:        true,
		SnapshotEntries:    uint64(d.cnf.DataSnapshotEntries),
		CompactionOverhead: uint64(d.cnf.DataCompactionOverhead),
		ClusterID:          shardID,
	}

	initialMembers := make(map[uint64]string)
	for _, nodeID := range nodeIDs {
		initialMembers[uint64(nodeID+1)] = d.cnf.RaftAddresses[nodeID]
	}

	createSMFunc := func(_ uint64, _ uint64) statemachine.IOnDiskStateMachine {
		sm := newShardODStateMachine(d, shardID, d.cnf.NodeID, nodeIDs)
		return sm
	}

	if err := d.nh.StartOnDiskCluster(initialMembers, false, createSMFunc, rc); err != nil {
		ch <- errors.WithStack(err)
		return
	}
	ch <- nil
}

func (d *Dragon) joinSequenceGroup() error {
	rc := config.Config{
		NodeID:             uint64(d.cnf.NodeID + 1),
		ElectionRTT:        20,
		HeartbeatRTT:       2,
		CheckQuorum:        true,
		SnapshotEntries:    uint64(d.cnf.SequenceSnapshotEntries),
		CompactionOverhead: uint64(d.cnf.SequenceCompactionOverhead),
		ClusterID:          tableSequenceClusterID,
	}

	initialMembers := make(map[uint64]string)
	// Dragonboat nodes must start at 1, zero is not allowed
	for i := 0; i < sequenceGroupSize; i++ {
		initialMembers[uint64(i+1)] = d.cnf.RaftAddresses[i]
	}
	if err := d.nh.StartOnDiskCluster(initialMembers, false, d.newSequenceODStateMachine, rc); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (d *Dragon) joinLockGroup() error {
	rc := config.Config{
		NodeID:             uint64(d.cnf.NodeID + 1),
		ElectionRTT:        20,
		HeartbeatRTT:       2,
		CheckQuorum:        true,
		SnapshotEntries:    uint64(d.cnf.LocksSnapshotEntries),
		CompactionOverhead: uint64(d.cnf.LocksCompactionOverhead),
		ClusterID:          locksClusterID,
	}

	initialMembers := make(map[uint64]string)
	// Dragonboat nodes must start at 1, zero is not allowed
	for i := 0; i < locksGroupSize; i++ {
		initialMembers[uint64(i+1)] = d.cnf.RaftAddresses[i]
	}
	if err := d.nh.StartOnDiskCluster(initialMembers, false, d.newLocksODStateMachine, rc); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

// For now, the number of shards and nodes in the cluster is static so we can just calculate this on startup
// on each node
func (d *Dragon) generateNodesAndShards(numShards int, replicationFactor int) {
	numNodes := len(d.cnf.RaftAddresses)
	allNodes := make([]int, numNodes)
	for i := 0; i < numNodes; i++ {
		allNodes[i] = i
	}
	d.localShards = make([]uint64, 0)
	d.allShards = make([]uint64, numShards)
	d.shardAllocs = make(map[uint64][]int)
	for i := 0; i < numShards; i++ {
		shardID := cluster.DataShardIDBase + uint64(i)
		d.allShards[i] = shardID
		nids := make([]int, replicationFactor)
		for j := 0; j < replicationFactor; j++ {
			nids[j] = allNodes[(i+j)%numNodes]
			if nids[j] == d.cnf.NodeID {
				d.localShards = append(d.localShards, shardID)
			}
		}
		d.shardAllocs[shardID] = nids
	}
}

// We deserialize into simple slices for puts and deletes as we don't need the actual WriteBatch instance in the
// state machine
func deserializeWriteBatch(buff []byte, offset int) (puts []cluster.KVPair, deletes [][]byte) {
	numPuts, offset := common.ReadUint32FromBufferLE(buff, offset)
	puts = make([]cluster.KVPair, numPuts)
	for i := 0; i < int(numPuts); i++ {
		var kl uint32
		kl, offset = common.ReadUint32FromBufferLE(buff, offset)
		kLen := int(kl)
		k := buff[offset : offset+kLen]
		offset += kLen
		var vl uint32
		vl, offset = common.ReadUint32FromBufferLE(buff, offset)
		vLen := int(vl)
		v := buff[offset : offset+vLen]
		offset += vLen
		puts[i] = cluster.KVPair{
			Key:   k,
			Value: v,
		}
	}
	numDeletes, offset := common.ReadUint32FromBufferLE(buff, offset)
	deletes = make([][]byte, numDeletes)
	for i := 0; i < int(numDeletes); i++ {
		var kl uint32
		kl, offset = common.ReadUint32FromBufferLE(buff, offset)
		kLen := int(kl)
		k := buff[offset : offset+kLen]
		offset += kLen
		deletes[i] = k
	}
	return
}

// It's expected to get cluster not ready from time to time, we should retry in this case
// See https://github.com/lni/dragonboat/issues/183
func (d *Dragon) executeWithRetry(f func() (interface{}, error), timeout time.Duration) (interface{}, error) {
	start := time.Now()
	for {
		res, err := f()
		if err == nil {
			return res, nil
		}
		if !errors.Is(err, dragonboat.ErrClusterNotReady) && !errors.Is(err, dragonboat.ErrTimeout) {
			return nil, errors.WithStack(err)
		}
		if time.Now().Sub(start) >= timeout {
			// If we timeout, then something is seriously wrong - we should just exit
			log.Errorf("timeout in making dragonboat calls, exiting %+v", err)
			common.DumpStacks()
			os.Exit(1)
			return nil, nil
		}
		time.Sleep(retryDelay)
	}
}

func (d *Dragon) proposeWithRetry(session *client.Session, cmd []byte) (statemachine.Result, error) {
	r, err := d.executeWithRetry(func() (interface{}, error) {
		ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
		res, err := d.nh.SyncPropose(ctx, session, cmd)
		cancel()
		return res, errors.WithStack(err)
	}, retryTimeout)
	if err != nil {
		return statemachine.Result{}, errors.WithStack(err)
	}
	smRes, ok := r.(statemachine.Result)
	if !ok {
		panic(fmt.Sprintf("not a sm result %v", smRes))
	}
	return smRes, errors.WithStack(err)
}

func (d *Dragon) AddToDeleteBatch(deleteBatch *cluster.ToDeleteBatch) error {
	batch := d.pebble.NewBatch()
	for _, prefix := range deleteBatch.Prefixes {
		key := createToDeleteKey(deleteBatch.Local, deleteBatch.ConditionalTableID, prefix)
		if err := batch.Set(key, nil, nil); err != nil {
			return errors.WithStack(err)
		}
	}
	if err := d.pebble.Apply(batch, nosyncWriteOptions); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (d *Dragon) RemoveToDeleteBatch(deleteBatch *cluster.ToDeleteBatch) error {
	batch := d.pebble.NewBatch()
	for _, prefix := range deleteBatch.Prefixes {
		key := createToDeleteKey(deleteBatch.Local, deleteBatch.ConditionalTableID, prefix)
		if err := batch.Delete(key, nil); err != nil {
			return errors.WithStack(err)
		}
	}
	if err := d.pebble.Apply(batch, nosyncWriteOptions); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

const localFlagTrue byte = 1
const localFlagFalse byte = 0

func createToDeleteKey(local bool, conditionalTableID uint64, prefix []byte) []byte {
	key := table.EncodeTableKeyPrefix(common.ToDeleteTableID, toDeleteShardID, 16+1+8+len(prefix))
	if local {
		key = append(key, localFlagTrue)
	} else {
		key = append(key, localFlagFalse)
	}
	key = common.AppendUint64ToBufferBE(key, conditionalTableID)
	key = append(key, prefix...)
	return key
}

func (d *Dragon) PostStartChecks(queryExec common.SimpleQueryExec) error {
	return d.checkDeleteToDeleteData(queryExec)
}

func (d *Dragon) checkDeleteToDeleteData(queryExec common.SimpleQueryExec) error { //nolint:gocyclo
	log.Debug("Checking for any to_delete data to delete")
	keyStartPrefix := table.EncodeTableKeyPrefix(common.ToDeleteTableID, toDeleteShardID, 16)
	keyEndPrefix := table.EncodeTableKeyPrefix(common.ToDeleteTableID+1, toDeleteShardID, 16)

	kvPairs, err := d.LocalScan(keyStartPrefix, keyEndPrefix, -1)
	if err != nil {
		return errors.WithStack(err)
	}

	var localBatches []*cluster.ToDeleteBatch
	var remoteBatches []*cluster.ToDeleteBatch
	var currBatch *cluster.ToDeleteBatch

	log.Debugf("There are %d to_delete entries", len(kvPairs))

	for _, kvPair := range kvPairs {
		offset := 16
		b := kvPair.Key[offset]
		offset++
		local := b == localFlagTrue
		conditionalTableID, _ := common.ReadUint64FromBufferBE(kvPair.Key, offset)
		offset += 8
		prefix := kvPair.Key[offset:]

		if currBatch == nil || currBatch.ConditionalTableID != conditionalTableID {
			currBatch = &cluster.ToDeleteBatch{
				Local:              local,
				ConditionalTableID: conditionalTableID,
			}
			if local {
				localBatches = append(localBatches, currBatch)
			} else {
				remoteBatches = append(remoteBatches, currBatch)
			}
		}
		currBatch.Prefixes = append(currBatch.Prefixes, prefix)
	}

	if len(localBatches) != 0 {
		// Process the local deletes
		pBatch := d.pebble.NewBatch()
		for _, batch := range localBatches {
			exists, err := d.tableExists(queryExec, batch.ConditionalTableID)
			if err != nil {
				return err
			}
			if !exists {
				for _, prefix := range batch.Prefixes {
					log.Debugf("Deleting all local data with prefix %s", common.DumpDataKey(prefix))
					endPrefix := common.IncrementBytesBigEndian(prefix)
					if err := d.deleteAllDataInRangeLocally(pBatch, prefix, endPrefix); err != nil {
						return err
					}
				}
			} else {
				// Conditional key exists - this means we do not delete the data
				log.Debugf("Not deleting to_delete data as table with id %d exists", batch.ConditionalTableID)
			}
		}
		if err := d.pebble.Apply(pBatch, nosyncWriteOptions); err != nil {
			return errors.WithStack(err)
		}
	}

	if len(remoteBatches) != 0 {
		// Process the replicated deletes
		for _, batch := range remoteBatches {
			exists, err := d.tableExists(queryExec, batch.ConditionalTableID)
			if err != nil {
				return err
			}
			if !exists {
				log.Debugf("Table does not exist, there are %d prefixes", len(batch.Prefixes))
				for _, prefix := range batch.Prefixes {
					log.Debugf("Deleting all remote data with prefix %s", common.DumpDataKey(prefix))
					endPrefix := common.IncrementBytesBigEndian(prefix)
					// shard id is first 8 bytes
					shardID, _ := common.ReadUint64FromBufferBE(prefix, 0)
					if err := d.deleteAllDataInRangeForShardFullPrefix(shardID, prefix, endPrefix); err != nil {
						return err
					}
				}
			} else {
				log.Debug("Table exists so not deleting table data")
			}
		}
	}

	// Now remove from to_delete table
	for _, batch := range localBatches {
		if err := d.RemoveToDeleteBatch(batch); err != nil {
			return err
		}
	}
	for _, batch := range remoteBatches {
		if err := d.RemoveToDeleteBatch(batch); err != nil {
			return err
		}
	}

	return nil
}

func (d *Dragon) tableExists(queryExec common.SimpleQueryExec, id uint64) (bool, error) {
	sql := fmt.Sprintf("select * from tables where id=%d", id)
	rows, err := queryExec.ExecuteQuery("sys", sql)
	if err != nil {
		return false, err
	}
	return rows.RowCount() != 0, nil
}

// Currently the number of shards in the cluster is fixed. We must make sure the user doesn't change the number of shards
// in the config after the cluster has been created. So we store the number in the database and check
func (d *Dragon) checkConstantShards(expectedShards int) error {
	log.Debugf("Checking constant shards: %d", expectedShards)
	key := table.EncodeTableKeyPrefix(common.LocalConfigTableID, 0, 16)
	propKey := []byte("num-shards")
	key = common.AppendUint32ToBufferBE(key, uint32(len(propKey)))
	key = append(key, propKey...)
	v, err := d.LocalGet(key)
	if err != nil {
		return err
	}
	if v == nil {
		log.Debug("New cluster - no value for num-shards in storage, persisting it")
		// New cluster - persist the value
		v = common.AppendUint32ToBufferBE([]byte{}, uint32(expectedShards))
		batch := d.pebble.NewBatch()
		if err := batch.Set(key, v, nil); err != nil {
			return errors.WithStack(err)
		}
		if err := d.pebble.Apply(batch, syncWriteOptions); err != nil {
			return errors.WithStack(err)
		}
	} else {
		shards, _ := common.ReadUint32FromBufferBE(v, 0)
		log.Debugf("value for num-shards found in storage: %d", shards)
		if int(shards) != expectedShards {
			return errors.Errorf("number of shards in cluster cannot be changed after cluster creation. cluster value %d new value %d", expectedShards, shards)
		}
	}
	return nil
}

func (d *Dragon) registerShardSM(shardID uint64) {
	if d.cnf.DisableShardPlacementSanityCheck {
		return
	}
	d.shardSMsLock.Lock()
	defer d.shardSMsLock.Unlock()
	if _, ok := d.shardSMs[shardID]; ok {
		panic(fmt.Sprintf("Shard SM for shard %d already registered on node %d", shardID, d.cnf.NodeID))
	}
	d.shardSMs[shardID] = struct{}{}
}

func (d *Dragon) unregisterShardSM(shardID uint64) {
	if d.cnf.DisableShardPlacementSanityCheck {
		return
	}
	d.shardSMsLock.Lock()
	defer d.shardSMsLock.Unlock()
	delete(d.shardSMs, shardID)
}

// ISystemEventListener implementation

func (d *Dragon) NodeHostShuttingDown() {
}

func (d *Dragon) NodeUnloaded(info raftio.NodeInfo) {
}

func (d *Dragon) NodeReady(info raftio.NodeInfo) {
}

func (d *Dragon) MembershipChanged(info raftio.NodeInfo) {
}

func (d *Dragon) ConnectionEstablished(info raftio.ConnectionInfo) {
}

func (d *Dragon) ConnectionFailed(info raftio.ConnectionInfo) {
}

func (d *Dragon) SendSnapshotStarted(info raftio.SnapshotInfo) {
}

func (d *Dragon) SendSnapshotCompleted(info raftio.SnapshotInfo) {
}

func (d *Dragon) SendSnapshotAborted(info raftio.SnapshotInfo) {
}

func (d *Dragon) SnapshotReceived(info raftio.SnapshotInfo) {
}

func (d *Dragon) SnapshotRecovered(info raftio.SnapshotInfo) {
}

func (d *Dragon) SnapshotCreated(info raftio.SnapshotInfo) {
}

func (d *Dragon) SnapshotCompacted(info raftio.SnapshotInfo) {
}

func (d *Dragon) LogCompacted(info raftio.EntryInfo) {
}

func (d *Dragon) LogDBCompacted(info raftio.EntryInfo) {
}
