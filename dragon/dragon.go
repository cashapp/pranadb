package dragon

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/cockroachdb/pebble"
	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/config"
	"github.com/lni/dragonboat/v3/raftio"
	"github.com/lni/dragonboat/v3/statemachine"
	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/common"
	"log"
	"path/filepath"
	"sync"
	"time"
)

const (
	// We reserve the first 1000 dragon cluster ids for system raft groups
	// The shard raft groups start from this value
	shardClusterIDBase uint64 = 1000

	// The dragon cluster id for the table sequence
	tableSequenceClusterID uint64 = 1

	// Timeout on dragon calls
	dragonCallTimeout = time.Second * 5
)

func NewDragon(nodeID int, nodeAddresses []string, totShards int, dataDir string, replicationFactor int) (cluster.Cluster, error) {
	if len(nodeAddresses) < 3 {
		return nil, errors.New("minimum cluster size is 3 nodes")
	}
	dragon := Dragon{
		nodeID:        nodeID,
		nodeAddresses: nodeAddresses,
		totShards:     totShards,
		dataDir:       dataDir,
	}
	dragon.generateNodesAndShards(totShards, replicationFactor)
	return &dragon, nil
}

type Dragon struct {
	lock                 sync.RWMutex
	nodeID               int
	nodeAddresses        []string
	totShards            int
	dataDir              string
	pebble               *pebble.DB
	nh                   *dragonboat.NodeHost
	shardAllocs          map[uint64][]int
	allShards            []uint64
	localShards          []uint64
	allNodes             []int // TODO probably don't need this
	started              bool
	shardListenerFactory cluster.ShardListenerFactory
}

func (d *Dragon) RegisterShardListenerFactory(factory cluster.ShardListenerFactory) {
	d.shardListenerFactory = factory
}

func (d *Dragon) GetNodeID() int {
	d.lock.Lock()
	defer d.lock.Unlock()
	return d.nodeID
}

func (d *Dragon) GetAllNodeIDs() []int {
	d.lock.Lock()
	defer d.lock.Unlock()
	return d.allNodes
}

func (d *Dragon) GenerateTableID() (uint64, error) {
	cs := d.nh.GetNoOPSession(tableSequenceClusterID)

	ctx, cancel := context.WithTimeout(context.Background(), dragonCallTimeout)
	defer cancel()

	var buff []byte
	buff = common.EncodeString("table", buff)

	proposeRes, err := d.nh.SyncPropose(ctx, cs, buff)
	if err != nil {
		return 0, err
	}
	if proposeRes.Value != seqStateMachineUpdatedOK {
		return 0, fmt.Errorf("unexpected return value from writing sequence: %d", proposeRes.Value)
	}
	seqBuff := proposeRes.Data
	seqVal := common.ReadUint64FromBufferLittleEndian(seqBuff, 0)
	return seqVal, nil
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

func (d *Dragon) ExecuteRemotePullQuery(schemaName string, query string, queryID string, limit int, nodeID int) chan cluster.RemoteQueryResult {
	// TODO
	return nil
}

func (d *Dragon) SetRemoteQueryExecutionCallback(callback cluster.RemoteQueryExecutionCallback) {
	// TODO
}

func (d *Dragon) Start() error {
	d.lock.Lock()
	defer d.lock.Unlock()
	if d.started {
		return nil
	}

	log.Printf("attempting to start dragon node %d", d.nodeID)

	datadir := filepath.Join(d.dataDir, fmt.Sprintf("node-%d", d.nodeID))
	pebbleDir := filepath.Join(datadir, "pebble")

	pebbleOptions := &pebble.Options{}
	pebble, err := pebble.Open(pebbleDir, pebbleOptions)
	if err != nil {
		return err
	}
	d.pebble = pebble

	nodeAddress := d.nodeAddresses[d.nodeID]

	dragonBoatDir := filepath.Join(datadir, "dragon")

	nhc := config.NodeHostConfig{
		WALDir:              dragonBoatDir,
		NodeHostDir:         dragonBoatDir,
		RTTMillisecond:      200,
		RaftAddress:         nodeAddress,
		SystemEventListener: d,
	}

	log.Printf("Attempting to create node host for node %d", d.nodeID)
	nh, err := dragonboat.NewNodeHost(nhc)
	if err != nil {
		return err
	}
	log.Printf("Node host for node %d created ok", d.nodeID)
	d.nh = nh

	err = d.joinSequenceGroup()
	if err != nil {
		return err
	}

	err = d.joinShardGroups()
	if err != nil {
		return err
	}

	// We now sleep for a while as leader election can continue some time after start as nodes
	// are started which causes a lot of churn
	// TODO think of a better way of doing this
	time.Sleep(5 * time.Second)

	d.started = true
	log.Printf("Dragon node %d started", d.nodeID)
	return nil
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
	return err
}

func (d *Dragon) WriteBatch(batch *cluster.WriteBatch) error {
	if batch.ShardID < shardClusterIDBase {
		panic("invalid shard cluster id")
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
	buff = serializeWriteBatch(batch, buff)

	proposeRes, err := d.nh.SyncPropose(ctx, cs, buff)
	if err != nil {
		return err
	}
	if proposeRes.Value != shardStateMachineResponseOK {
		return fmt.Errorf("unexpected return value from writing batch: %d to shard %d", proposeRes.Value, batch.ShardID)
	}

	return nil
}

func (d *Dragon) LocalGet(key []byte) ([]byte, error) {
	return localGet(d.pebble, key)
}

func (d *Dragon) LocalScan(startKeyPrefix []byte, whileKeyPrefix []byte, limit int) ([]cluster.KVPair, error) {

	s1 := string(startKeyPrefix)
	s2 := string(whileKeyPrefix)
	log.Printf("start:%s while:%s", s1, s2)

	iterOptions := &pebble.IterOptions{}
	iter := d.pebble.NewIter(iterOptions)
	iter.SeekGE(startKeyPrefix)
	whilePrefixLen := len(whileKeyPrefix)
	count := 0
	var pairs []cluster.KVPair
	if iter.Valid() {
		for limit == -1 || count < limit {
			k := iter.Key()
			sk := string(k)
			log.Printf("key is:%s", sk)
			if bytes.Compare(k[0:whilePrefixLen], whileKeyPrefix) > 0 {
				log.Println("not adding that one!")
				break
			}
			v := iter.Value()
			pairs = append(pairs, cluster.KVPair{
				Key:   copySlice(k),
				Value: copySlice(v),
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

func localGet(peb *pebble.DB, key []byte) ([]byte, error) {
	v, closer, err := peb.Get(key)
	if err == pebble.ErrNotFound {
		return nil, nil
	}
	res := copySlice(v)
	if closer != nil {
		err = closer.Close()
		if err != nil {
			return nil, err
		}
	}
	return res, nil
}

// Pebble tends to reuse buffers so we generally have to copy them before using them elsewhere
// or the underlying bytes can change
func copySlice(buff []byte) []byte {
	res := make([]byte, len(buff))
	copy(res, buff)
	return res
}

func (d *Dragon) joinShardGroups() error {
	chans := make([]chan error, len(d.shardAllocs))
	index := 0
	for shardID, nodeIDs := range d.shardAllocs {
		ch := make(chan error, 1)
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
		NodeID:             uint64(d.nodeID + 1),
		ElectionRTT:        5,
		HeartbeatRTT:       1,
		CheckQuorum:        true,
		SnapshotEntries:    10,
		CompactionOverhead: 5,
		ClusterID:          shardID,
	}

	initialMembers := make(map[uint64]string)
	for _, nodeID := range nodeIDs {
		initialMembers[uint64(nodeID+1)] = d.nodeAddresses[nodeID]
	}

	log.Printf("Node %d attempting to join shard cluster %d", d.nodeID, shardID)

	createSMFunc := func(_ uint64, _ uint64) statemachine.IStateMachine {
		return newShardStateMachine(d, shardID, d.nodeID, nodeIDs)
	}
	if err := d.nh.StartCluster(initialMembers, false, createSMFunc, rc); err != nil {
		ch <- fmt.Errorf("failed to start shard dragonboat cluster %v", err)
		return
	}
	log.Printf("Node %d successfully joined shard cluster %d", d.nodeID, shardID)
	ch <- nil
}

func (d *Dragon) joinSequenceGroup() error {
	rc := config.Config{
		NodeID:             uint64(d.nodeID + 1),
		ElectionRTT:        5,
		HeartbeatRTT:       1,
		CheckQuorum:        true,
		SnapshotEntries:    10,
		CompactionOverhead: 5,
		ClusterID:          tableSequenceClusterID,
	}

	initialMembers := make(map[uint64]string)
	// Dragonboat nodes must start at 1, zero is not allowed
	// We take the first 3 nodes
	for i := 0; i < 3; i++ {
		initialMembers[uint64(i+1)] = d.nodeAddresses[i]
	}
	log.Printf("Node %d attempting to join sequence cluster", d.nodeID)
	if err := d.nh.StartCluster(initialMembers, false, d.newSequenceStateMachine, rc); err != nil {
		return fmt.Errorf("failed to start sequence dragonboat cluster %v", err)
	}
	log.Printf("Node %d successfully joined sequence cluster", d.nodeID)
	return nil
}

// For now, the number of shards and nodes in the cluster is static so we can just calculate this on startup
// on each node
func (d *Dragon) generateNodesAndShards(numShards int, replicationFactor int) {
	numNodes := len(d.nodeAddresses)
	d.allNodes = make([]int, numNodes)
	for i := 0; i < numNodes; i++ {
		d.allNodes[i] = i
	}
	d.localShards = make([]uint64, 0)
	d.allShards = make([]uint64, numShards)
	d.shardAllocs = make(map[uint64][]int)
	for i := 0; i < numShards; i++ {
		shardID := shardClusterIDBase + uint64(i)
		d.allShards[i] = shardID
		nids := make([]int, replicationFactor)
		for j := 0; j < replicationFactor; j++ {
			nids[j] = d.allNodes[(i+j)%numNodes]
			if nids[j] == d.nodeID {
				d.localShards = append(d.localShards, shardID)
			}
		}
		d.shardAllocs[shardID] = nids
	}
}

// A node in the cluster died, we need to tell all relevant shard state machines to remove the node
func (d *Dragon) nodeDied(nodeID int) {
	// TODO
}

func serializeWriteBatch(wb *cluster.WriteBatch, buff []byte) []byte {
	buff = common.AppendUint32ToBufferLittleEndian(buff, uint32(len(wb.Puts.TheMap)))
	for k, v := range wb.Puts.TheMap {
		kb := common.StringToByteSliceZeroCopy(k)
		buff = common.AppendUint32ToBufferLittleEndian(buff, uint32(len(kb)))
		buff = append(buff, kb...)
		buff = common.AppendUint32ToBufferLittleEndian(buff, uint32(len(v)))
		buff = append(buff, v...)
	}
	buff = common.AppendUint32ToBufferLittleEndian(buff, uint32(len(wb.Deletes.TheMap)))
	for k := range wb.Deletes.TheMap {
		kb := common.StringToByteSliceZeroCopy(k)
		buff = common.AppendUint32ToBufferLittleEndian(buff, uint32(len(kb)))
		buff = append(buff, kb...)
	}
	return buff
}

func deserializeWriteBatch(buff []byte, offset int) (puts []cluster.KVPair, deletes [][]byte) {
	numPuts := common.ReadUint32FromBufferLittleEndian(buff, offset)
	offset += 4
	puts = make([]cluster.KVPair, numPuts)
	for i := 0; i < int(numPuts); i++ {
		kLen := int(common.ReadUint32FromBufferLittleEndian(buff, offset))
		offset += 4
		k := buff[offset : offset+kLen]
		offset += kLen
		vLen := int(common.ReadUint32FromBufferLittleEndian(buff, offset))
		offset += 4
		v := buff[offset : offset+vLen]
		offset += vLen
		puts[i] = cluster.KVPair{
			Key:   k,
			Value: v,
		}
	}
	numDeletes := common.ReadUint32FromBufferLittleEndian(buff, offset)
	offset += 4
	deletes = make([][]byte, numDeletes)
	for i := 0; i < int(numDeletes); i++ {
		kLen := int(common.ReadUint32FromBufferLittleEndian(buff, offset))
		offset += 4
		k := buff[offset : offset+kLen]
		offset += kLen
		deletes[i] = k
	}
	return
}

func (d *Dragon) nodeRemovedFromCluster(nodeID int, shardID uint64) error {
	if shardID < shardClusterIDBase {
		// Ignore - these are not writing nodes
		return nil
	}

	cs := d.nh.GetNoOPSession(shardID)

	ctx, cancel := context.WithTimeout(context.Background(), dragonCallTimeout)
	defer cancel()
	var buff []byte
	buff = append(buff, shardStateMachineCommandRemoveNode)
	buff = common.AppendUint32ToBufferLittleEndian(buff, uint32(nodeID))

	proposeRes, err := d.nh.SyncPropose(ctx, cs, buff)

	if err != nil {
		return err
	}
	if proposeRes.Value != shardStateMachineResponseOK {
		return fmt.Errorf("unexpected return value from removing node: %d to shard %d", proposeRes.Value, shardID)
	}
	return nil
}

func (d *Dragon) NodeHostShuttingDown() {
}

func (d *Dragon) NodeUnloaded(info raftio.NodeInfo) {
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
