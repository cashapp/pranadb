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

	// The dragon cluster id for the shard allocation information (i.e. which shards on which nodes)
	shardAllocationClusterID uint64 = 0

	// The dragon cluster id for the table sequence
	tableSequenceClusterID uint64 = 1
)

func NewDragon(nodeID int, nodeAddresses []string, totShards int, dataDir string, replicationFactor int) (cluster.Cluster, error) {
	if len(nodeAddresses) < 3 {
		return nil, errors.New("minimum cluster size is 3 nodes")
	}
	return &Dragon{
		nodeID:            nodeID,
		nodeAddresses:     nodeAddresses,
		totShards:         totShards,
		dataDir:           dataDir,
		replicationFactor: replicationFactor,
		leaderShards:      make(map[uint64]bool),
		allShards:         genAllShardIds(totShards),
		allNodes:          genAllNodes(len(nodeAddresses)),
	}, nil
}

type Dragon struct {
	lock                  sync.RWMutex
	nodeID                int
	nodeAddresses         []string
	totShards             int
	dataDir               string
	pebble                *pebble.DB
	replicationFactor     int
	nh                    *dragonboat.NodeHost
	shardAllocations      *shardAllocation
	leaderChangedCallback cluster.LeaderChangeCallback
	leaderShards          map[uint64]bool
	remoteWriteHandler    cluster.RemoteWriteHandler
	allShards             []uint64
	allNodes              []int // TODO probably don't need this
	started               bool
}

type shardAllocation struct {
	allocations map[int][]uint64
}

func (d *Dragon) GetNodeID() int {
	return d.nodeID
}

func (d *Dragon) GetAllNodeIDs() []int {
	return d.allNodes
}

func (d *Dragon) GenerateTableID() (uint64, error) {
	cs := d.nh.GetNoOPSession(tableSequenceClusterID)
	// TODO configurable timeout
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)

	var buff []byte
	buff = common.EncodeString("table", buff)

	proposeRes, err := d.nh.SyncPropose(ctx, cs, buff)
	cancel()
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

func (d *Dragon) SetLeaderChangedCallback(callback cluster.LeaderChangeCallback) {
	d.lock.Lock()
	defer d.lock.Unlock()
	if d.started {
		// Must be set before starting or can miss state changes
		panic("Cannot set leader change callback after dragon is started")
	}
	d.leaderChangedCallback = callback
}

func (d *Dragon) GetAllShardIDs() []uint64 {
	return d.allShards
}

func (d *Dragon) ExecuteRemotePullQuery(schemaName string, query string, queryID string, limit int, nodeID int) chan cluster.RemoteQueryResult {
	panic("implement me")
}

func (d *Dragon) SetRemoteQueryExecutionCallback(callback cluster.RemoteQueryExecutionCallback) {
	panic("implement me")
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
		WALDir:            dragonBoatDir,
		NodeHostDir:       dragonBoatDir,
		RTTMillisecond:    200,
		RaftAddress:       nodeAddress,
		RaftEventListener: d,
	}

	log.Printf("Attempting to create node host for node %d", d.nodeID)
	nh, err := dragonboat.NewNodeHost(nhc)
	if err != nil {
		return err
	}
	log.Printf("Node host for node %d created ok", d.nodeID)
	d.nh = nh

	allocation, err := d.syncShardAllocations()
	if err != nil {
		return err
	}
	d.shardAllocations = allocation

	err = d.joinSequenceGroup()
	if err != nil {
		return err
	}

	err = d.joinShardGroups()
	if err != nil {
		return err
	}

	// We now sleep for a while as leader election can continue some time after start as nodes
	// are started which causes a lot of churn in what nodes have what leaders and subsequently
	// what schedulers are started on each node
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

	// TODO is it right to get NOOP session?
	// TODO should we get a new client each time - or reuse them?
	cs := d.nh.GetNoOPSession(batch.ShardID)
	// TODO configurable timeout
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)

	var buff []byte
	buff = serializeWriteBatch(batch, buff)

	proposeRes, err := d.nh.SyncPropose(ctx, cs, buff)
	cancel()
	if err != nil {
		return err
	}
	if proposeRes.Value != shardStateMachineUpdatedOK {
		return fmt.Errorf("unexpected return value from writing batch: %d", proposeRes.Value)
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
				// TODO we might be able to optimise away the copies
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

func (d *Dragon) SetRemoteWriteHandler(handler cluster.RemoteWriteHandler) {
	// TODO atomic reference
	d.lock.Lock()
	defer d.lock.Unlock()
	d.remoteWriteHandler = handler
}

func (d *Dragon) LeaderUpdated(info raftio.LeaderInfo) {
	d.lock.Lock()
	defer d.lock.Unlock()
	if info.ClusterID >= shardClusterIDBase {
		log.Printf("leader updated node id %d shard id %d this node id %d leader id is %d", info.NodeID, info.ClusterID, d.nodeID+1, info.LeaderID)
		if d.nodeID+1 == int(info.LeaderID) {
			// This node became leader
			d.leaderShards[info.ClusterID] = true
			if d.leaderChangedCallback != nil {
				d.leaderChangedCallback.LeaderChanged(info.ClusterID, true)
			}
		} else {
			// Another node became leader
			_, wasLeader := d.leaderShards[info.ClusterID]
			if wasLeader {
				delete(d.leaderShards, info.ClusterID)
				if d.leaderChangedCallback != nil {
					d.leaderChangedCallback.LeaderChanged(info.ClusterID, false)
				}
			}
		}
	}
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

func genAllShardIds(numShards int) []uint64 {
	allShards := make([]uint64, numShards)
	for i := 0; i < numShards; i++ {
		allShards[i] = shardClusterIDBase + uint64(i)
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

func (sa *shardAllocation) serialize(buff []byte) []byte {
	buff = common.AppendUint32ToBufferLittleEndian(buff, uint32(len(sa.allocations)))
	for nodeID, shardIDs := range sa.allocations {
		buff = common.AppendUint32ToBufferLittleEndian(buff, uint32(nodeID))
		buff = common.AppendUint32ToBufferLittleEndian(buff, uint32(len(shardIDs)))
		for _, shardID := range shardIDs {
			buff = common.AppendUint64ToBufferLittleEndian(buff, shardID)
		}
	}
	return buff
}

func (sa *shardAllocation) deserialize(buff []byte) {
	offset := 0
	numAllocations := common.ReadUint32FromBufferLittleEndian(buff, offset)
	offset += 4
	sa.allocations = make(map[int][]uint64, numAllocations)
	for i := 0; i < int(numAllocations); i++ {
		nodeID := common.ReadUint32FromBufferLittleEndian(buff, offset)
		offset += 4
		numShards := common.ReadUint32FromBufferLittleEndian(buff, offset)
		offset += 4
		shards := make([]uint64, numShards)
		for j := 0; j < int(numShards); j++ {
			shardID := common.ReadUint64FromBufferLittleEndian(buff, offset)
			offset += 8
			shards[j] = shardID
		}
		sa.allocations[int(nodeID)] = shards
	}
}

func (d *Dragon) joinShardGroups() error {
	shardMap := make(map[uint64][]int)
	for nodeID, shardIDs := range d.shardAllocations.allocations {
		for _, shardID := range shardIDs {
			nodeIDs, ok := shardMap[shardID]
			if !ok {
				nodeIDs = make([]int, 0)
			}
			nodeIDs = append(nodeIDs, nodeID)
			shardMap[shardID] = nodeIDs
		}
	}
	chans := make([]chan error, len(shardMap))
	index := 0
	for shardID, nodeIDs := range shardMap {
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
	if err := d.nh.StartCluster(initialMembers, false, d.newShardStateMachine, rc); err != nil {
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

func (d *Dragon) syncShardAllocations() (*shardAllocation, error) {

	rc := config.Config{
		NodeID:             uint64(d.nodeID + 1),
		ElectionRTT:        5,
		HeartbeatRTT:       1,
		CheckQuorum:        true,
		SnapshotEntries:    10,
		CompactionOverhead: 5,
		ClusterID:          shardAllocationClusterID,
	}

	initialMembers := make(map[uint64]string)
	// Dragonboat nodes must start at 1, zero is not allowed
	// We take the first 3 nodes
	for i := 0; i < 3; i++ {
		initialMembers[uint64(i+1)] = d.nodeAddresses[i]
	}

	log.Printf("Node %d attempting to join shard allocation cluster", d.nodeID)
	if err := d.nh.StartCluster(initialMembers, false, d.newShardAllocationStateMachine, rc); err != nil {
		return nil, fmt.Errorf("failed to start cluster info dragonboat cluster %v", err)
	}
	log.Printf("Node %d successfully joined shard allocation cluster", d.nodeID)

	var allocation *shardAllocation

	for allocation == nil {
		// Try and get shard allocations
		var res interface{}
		for {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			var err error
			res, err = d.nh.SyncRead(ctx, shardAllocationClusterID, "")
			cancel()
			if err == nil {
				break
			}
			// It's expected to get an error until the cluster is ready - we retry
			// TODO add timeout
			time.Sleep(250 * time.Millisecond)
		}
		bytes, ok := res.([]byte)
		if !ok {
			return nil, fmt.Errorf("invalid clusterinfo %v", res)
		}
		allocation = &shardAllocation{}
		allocation.deserialize(bytes)

		if len(allocation.allocations) > 0 {
			break
		}

		// Try and initialise cluster allocations
		var err error
		allocation, err = d.initialiseShardAllocations(d.replicationFactor)
		if err != nil {
			return nil, err
		}
		var serialized []byte
		serialized = allocation.serialize(serialized)
		cs := d.nh.GetNoOPSession(shardAllocationClusterID)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		proposeRes, err := d.nh.SyncPropose(ctx, cs, serialized)
		cancel()
		if err != nil {
			return nil, err
		}
		if proposeRes.Value == shardAllocationUpdateSucceeded {
			// Update succeeded
			break
		}

		// Retry after delay
		time.Sleep(time.Millisecond * 250)
	}

	log.Printf("node %d got shard allocation ok %d", d.nodeID, len(allocation.allocations))
	return allocation, nil
}

// Distributes shards evenly across the cluster
func (d *Dragon) initialiseShardAllocations(replicationFactor int) (*shardAllocation, error) {
	numNodes := len(d.nodeAddresses)
	if replicationFactor > numNodes {
		return nil, errors.New("replication factor must be <= number of nodes in cluster")
	}
	allocation := &shardAllocation{}
	allocation.allocations = make(map[int][]uint64, numNodes)

	fAdd := func(nodeID int, shardID uint64) {
		shards, ok := allocation.allocations[nodeID]
		if !ok {
			shards = make([]uint64, 0)
		}
		shards = append(shards, shardID)
		allocation.allocations[nodeID] = shards
	}

	fGetNodeID := func(shardID uint64) int {
		return int(shardID-1) % numNodes
	}

	for shardID := shardClusterIDBase; shardID < uint64(d.totShards)+shardClusterIDBase; shardID++ {
		for j := 0; j < replicationFactor; j++ {
			fAdd(fGetNodeID(shardID+uint64(j)), shardID)
		}
	}

	return allocation, nil
}

func (d *Dragon) isLeader(shardID uint64) bool {
	d.lock.RLock()
	defer d.lock.RUnlock()
	_, ok := d.leaderShards[shardID]
	return ok
}

// TODO atomic refs
func (d *Dragon) callRemoteWriteHandler(shardID uint64) {
	d.lock.RLock()
	defer d.lock.RUnlock()
	if d.remoteWriteHandler != nil {
		d.remoteWriteHandler.RemoteWriteOccurred(shardID)
	}
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
