package dragon

import (
	"context"
	"fmt"
	"github.com/lni/dragonboat/v3/client"
	"github.com/squareup/pranadb/conf"
	"log"
	"path/filepath"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/config"
	"github.com/lni/dragonboat/v3/logger"
	"github.com/lni/dragonboat/v3/raftio"
	"github.com/lni/dragonboat/v3/statemachine"
	"github.com/pkg/errors"

	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/common"
)

const (
	// The dragon cluster id for the table sequence group
	tableSequenceClusterID uint64 = 1

	locksClusterID uint64 = 2

	// Timeout on dragon calls
	dragonCallTimeout = time.Second * 10

	// Timeout on remote queries
	remoteQueryTimeout = time.Second * 10

	sequenceGroupSize = 3

	locksGroupSize = 3

	retryDelay = 10 * time.Millisecond

	executeRetryTimeout = 10 * time.Second
)

func NewDragon(cnf conf.Config) (cluster.Cluster, error) {
	if len(cnf.RaftAddresses) < 3 {
		return nil, errors.New("minimum cluster size is 3 nodes")
	}
	dragon := Dragon{cnf: cnf}
	dragon.generateNodesAndShards(cnf.NumShards, cnf.ReplicationFactor)
	return &dragon, nil
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
	shuttingDown                 bool
	membershipListener           cluster.MembershipListener
}

func init() {
	// This should be customizable, but these are good defaults
	logger.GetLogger("dragonboat").SetLevel(logger.WARNING)
	logger.GetLogger("raft").SetLevel(logger.ERROR)
	logger.GetLogger("rsm").SetLevel(logger.ERROR)
	logger.GetLogger("transport").SetLevel(logger.WARNING)
	logger.GetLogger("grpc").SetLevel(logger.WARNING)
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

func (d *Dragon) GenerateTableID() (uint64, error) {
	cs := d.nh.GetNoOPSession(tableSequenceClusterID)

	ctx, cancel := context.WithTimeout(context.Background(), dragonCallTimeout)
	defer cancel()

	var buff []byte
	buff = common.AppendStringToBufferLE(buff, "table")

	proposeRes, err := d.proposeWithRetry(ctx, cs, buff)
	if err != nil {
		return 0, err
	}
	if proposeRes.Value != seqStateMachineUpdatedOK {
		return 0, fmt.Errorf("unexpected return value from writing sequence: %d", proposeRes.Value)
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

	ctx, cancel := context.WithTimeout(context.Background(), dragonCallTimeout)
	defer cancel()

	var buff []byte
	buff = common.AppendStringToBufferLE(buff, command)
	buff = common.AppendStringToBufferLE(buff, prefix)

	proposeRes, err := d.proposeWithRetry(ctx, cs, buff)
	if err != nil {
		return false, err
	}
	if proposeRes.Value != locksStateMachineUpdatedOK {
		return false, fmt.Errorf("unexpected return value from lock request: %d", proposeRes.Value)
	}
	resBuff := proposeRes.Data
	var bRes bool
	if res := resBuff[0]; res == LockSMResultTrue {
		bRes = true
	} else if res == LockSMResultFalse {
		bRes = false
	} else {
		return false, fmt.Errorf("unexpected return value from lock request %d", res)
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

	ctx, cancel := context.WithTimeout(context.Background(), remoteQueryTimeout)
	defer cancel()

	queryRequest, err := queryInfo.Serialize()
	if err != nil {
		return nil, err
	}

	res, err := d.executeWithRetry(func() (interface{}, error) {
		return d.nh.SyncRead(ctx, queryInfo.ShardID, queryRequest)
	})

	if err != nil {
		return nil, errors.WithStack(fmt.Errorf("failed to execute query on node %d %s %v", d.cnf.ClusterID, queryInfo.Query, err))
	}
	bytes, ok := res.([]byte)
	if !ok {
		panic("expected []byte")
	}

	rows := rowsFactory.NewRows(1)
	rows.Deserialize(bytes)

	return rows, nil
}

func (d *Dragon) SetRemoteQueryExecutionCallback(callback cluster.RemoteQueryExecutionCallback) {
	d.remoteQueryExecutionCallback = callback
}

func (d *Dragon) Start() error {
	d.lock.Lock()
	defer d.lock.Unlock()
	if d.started {
		return nil
	}

	if d.remoteQueryExecutionCallback == nil {
		panic("remote query execution callback must be set before start")
	}
	if d.shardListenerFactory == nil {
		panic("shard listener factory must be set before start")
	}

	d.shuttingDown = false

	datadir := filepath.Join(d.cnf.DataDir, fmt.Sprintf("node-%d", d.cnf.NodeID))
	pebbleDir := filepath.Join(datadir, "pebble")
	d.ingestDir = filepath.Join(datadir, "ingest-snapshots")

	// TODO used tuned config for Pebble - this can be copied from the Dragonboat Pebble config (see kv_pebble.go in Dragonboat)
	pebbleOptions := &pebble.Options{}
	pebble, err := pebble.Open(pebbleDir, pebbleOptions)
	if err != nil {
		return err
	}
	d.pebble = pebble

	nodeAddress := d.cnf.RaftAddresses[d.cnf.NodeID]

	dragonBoatDir := filepath.Join(datadir, "dragon")

	nhc := config.NodeHostConfig{
		DeploymentID:        uint64(d.cnf.ClusterID),
		WALDir:              dragonBoatDir,
		NodeHostDir:         dragonBoatDir,
		RTTMillisecond:      200,
		RaftAddress:         nodeAddress,
		SystemEventListener: d,
	}

	nh, err := dragonboat.NewNodeHost(nhc)
	if err != nil {
		return err
	}
	d.nh = nh

	err = d.joinSequenceGroup()
	if err != nil {
		return err
	}

	err = d.joinLockGroup()
	if err != nil {
		return err
	}

	err = d.joinShardGroups()
	if err != nil {
		return err
	}

	d.started = true
	log.Printf("Dragon node %d started", d.cnf.NodeID)
	return nil
}

func (d *Dragon) Stop() error {
	d.lock.Lock()
	defer d.lock.Unlock()
	if !d.started {
		return nil
	}
	d.shuttingDown = true
	d.nh.Stop()
	err := d.pebble.Close()
	if err == nil {
		d.started = false
	}
	return err
}

func (d *Dragon) WriteBatch(batch *cluster.WriteBatch) error {
	if batch.ShardID < cluster.DataShardIDBase {
		panic(fmt.Sprintf("invalid shard cluster id %d", batch.ShardID))
	}

	/*
		We use a NOOP session as we do not need duplicate detection at the Raft level

		Batches are written in two cases:
		1. A batch that handles some rows which have arrived in the receiver queue for a shard.
		In this case the batch contains
		   a) any writes made during processing of the rows
		   b) update of last received sequence for the rows
		   c) deletes from the receiver table
		If the call to write this batch fails, the batch may or may not have been committed reliably to raft.
		In the case it was not committed then the rows will stil be in the receiver table and last sequence not updated
		so they will be retried with no adverse effects.
		In the case they were committed, then last received sequence will have been updated and rows deleted from
		receiver table so retry won't reprocess them
		2. A batch that involves writing some rows into the forwarder queue after they have been received by a Kafka consumer
		(source). In this case if the call to write fails, the batch may actually have been committed and forwarded.
		If the Kafka offset has not been committed then on recover the same rows can be written again.
		To avoid this we will implement idempotency at the source level, and keep track, persistently of [partition_id, offset]
		for each partition received at each shard, and ignore rows if seen before when ingesting.

		So we don't need idempotent client sessions. Moreover, idempotency of client sessions in Raft times out after an hour(?)
		and does not survive restarts of the whole cluster.
	*/
	cs := d.nh.GetNoOPSession(batch.ShardID)

	ctx, cancel := context.WithTimeout(context.Background(), dragonCallTimeout)
	defer cancel()

	var buff []byte
	if batch.NotifyRemote {
		buff = append(buff, shardStateMachineCommandForwardWrite)
	} else {
		buff = append(buff, shardStateMachineCommandWrite)
	}
	buff = batch.Serialize(buff)

	proposeRes, err := d.proposeWithRetry(ctx, cs, buff)
	if err != nil {
		return err
	}
	if proposeRes.Value != shardStateMachineResponseOK {
		return fmt.Errorf("unexpected return value from writing batch: %d to shard %d %d", proposeRes.Value, batch.ShardID, proposeRes.Value)
	}

	return nil
}

func (d *Dragon) LocalGet(key []byte) ([]byte, error) {
	return localGet(d.pebble, key)
}

func (d *Dragon) LocalScan(startKeyPrefix []byte, endKeyPrefix []byte, limit int) ([]cluster.KVPair, error) {
	if startKeyPrefix == nil {
		panic("startKeyPrefix cannot be nil")
	}
	iterOptions := &pebble.IterOptions{LowerBound: startKeyPrefix, UpperBound: endKeyPrefix}
	iter := d.pebble.NewIter(iterOptions)
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
		return nil, err
	}
	return pairs, nil
}

func (d *Dragon) DeleteAllDataInRange(startPrefix []byte, endPrefix []byte) error {

	chans := make([]chan error, len(d.allShards))
	for i, shardID := range d.allShards {

		ch := make(chan error, 1)
		chans[i] = ch

		theShardID := shardID
		go func() {
			// Remember, key must be in big-endian order
			startPrefixWithShard := make([]byte, 0, 16)
			startPrefixWithShard = common.AppendUint64ToBufferBE(startPrefixWithShard, theShardID)
			startPrefixWithShard = append(startPrefixWithShard, startPrefix...)

			endPrefixWithShard := make([]byte, 0, 16)
			endPrefixWithShard = common.AppendUint64ToBufferBE(endPrefixWithShard, theShardID)
			endPrefixWithShard = append(endPrefixWithShard, endPrefix...)

			cs := d.nh.GetNoOPSession(theShardID)

			ctx, cancel := context.WithTimeout(context.Background(), dragonCallTimeout)
			defer cancel()

			var buff []byte
			buff = append(buff, shardStateMachineCommandDeleteRangePrefix)

			buff = common.AppendUint32ToBufferLE(buff, uint32(len(startPrefixWithShard)))
			buff = append(buff, startPrefixWithShard...)
			buff = common.AppendUint32ToBufferLE(buff, uint32(len(endPrefixWithShard)))
			buff = append(buff, endPrefixWithShard...)

			proposeRes, err := d.proposeWithRetry(ctx, cs, buff)
			if err != nil {
				ch <- err
			}
			if proposeRes.Value != shardStateMachineResponseOK {
				ch <- fmt.Errorf("unexpected return value %d from request to delete range to shard %d", proposeRes.Value, theShardID)
			}
			ch <- nil
		}()
	}

	for _, ch := range chans {
		err, ok := <-ch
		if !ok {
			panic("channel closed")
		}
		if err != nil {
			return err
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
		return nil, err
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
			return errors.New("channel was closed")
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *Dragon) joinShardGroup(shardID uint64, nodeIDs []int, ch chan error) {
	rc := config.Config{
		NodeID:             uint64(d.cnf.NodeID + 1),
		ElectionRTT:        10,
		HeartbeatRTT:       1,
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
		return newShardODStateMachine(d, shardID, d.cnf.NodeID, nodeIDs)
	}

	if err := d.nh.StartOnDiskCluster(initialMembers, false, createSMFunc, rc); err != nil {
		ch <- fmt.Errorf("failed to start shard dragonboat cluster %v", err)
		return
	}
	ch <- nil
}

func (d *Dragon) joinSequenceGroup() error {
	rc := config.Config{
		NodeID:             uint64(d.cnf.NodeID + 1),
		ElectionRTT:        10,
		HeartbeatRTT:       1,
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
		return fmt.Errorf("failed to start sequence dragonboat cluster %v", err)
	}
	return nil
}

func (d *Dragon) joinLockGroup() error {
	rc := config.Config{
		NodeID:             uint64(d.cnf.NodeID + 1),
		ElectionRTT:        10,
		HeartbeatRTT:       1,
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
		return fmt.Errorf("failed to start locks dragonboat cluster %v", err)
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

func (d *Dragon) nodeRemovedFromCluster(nodeID int, shardID uint64) error {
	if shardID < cluster.DataShardIDBase {
		// Ignore - these are not writing nodes
		return nil
	}

	cs := d.nh.GetNoOPSession(shardID)

	ctx, cancel := context.WithTimeout(context.Background(), dragonCallTimeout)
	defer cancel()
	var buff []byte
	buff = append(buff, shardStateMachineCommandRemoveNode)
	buff = common.AppendUint32ToBufferLE(buff, uint32(nodeID))

	proposeRes, err := d.proposeWithRetry(ctx, cs, buff)

	if err != nil {
		return err
	}
	if proposeRes.Value != shardStateMachineResponseOK {
		return fmt.Errorf("unexpected return value from removing node: %d to shard %d", proposeRes.Value, shardID)
	}

	d.membershipListener.NodeLeft(nodeID)

	return nil
}

// It's expected to get cluster not ready from time to time, we should retry in this case
// See https://github.com/lni/dragonboat/issues/183
func (d *Dragon) executeWithRetry(f func() (interface{}, error)) (interface{}, error) {
	start := time.Now()
	for {
		res, err := f()
		if err == nil {
			return res, nil
		}
		if err != dragonboat.ErrClusterNotReady {
			return nil, err
		}
		if time.Now().Sub(start) >= executeRetryTimeout {
			return nil, err
		}
		time.Sleep(retryDelay)
	}
}

func (d *Dragon) proposeWithRetry(ctx context.Context,
	session *client.Session, cmd []byte) (statemachine.Result, error) {
	r, err := d.executeWithRetry(func() (interface{}, error) {
		return d.nh.SyncPropose(ctx, session, cmd)
	})
	smRes, ok := r.(statemachine.Result)
	if !ok {
		panic(fmt.Sprintf("not a sm result %v", smRes))
	}
	return smRes, err
}

// ISystemEventListener implementation

func (d *Dragon) NodeHostShuttingDown() {
}

func (d *Dragon) NodeUnloaded(info raftio.NodeInfo) {
	if d.shuttingDown {
		return
	}
	go func() {
		err := d.nodeRemovedFromCluster(int(info.NodeID-1), info.ClusterID)
		if err != nil {
			log.Printf("failed to remove node from cluster %v", err)
		}
	}()
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
