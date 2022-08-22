package dragon

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lni/dragonboat/v3/logger"
	"github.com/squareup/pranadb/cluster/dragon/logadaptor"
	"github.com/squareup/pranadb/interruptor"
	"github.com/squareup/pranadb/protos/squareup/cash/pranadb/v1/clustermsgs"
	"github.com/squareup/pranadb/remoting"

	"github.com/squareup/pranadb/table"

	"github.com/lni/dragonboat/v3/client"
	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/conf"
	"github.com/squareup/pranadb/errors"

	"github.com/cockroachdb/pebble"
	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/config"
	"github.com/lni/dragonboat/v3/statemachine"
	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/common"

	"github.com/twinj/uuid"
)

const (
	// The dragon cluster id for the table sequence group
	tableSequenceClusterID uint64 = 1

	locksClusterID uint64 = 2

	callTimeout = 10 * time.Second

	toDeleteShardID uint64 = 4

	nodeHostStartTimeout = 10 * time.Second

	pullQueryRetryTimeout = 10 * time.Second

	// In practice, we retry all transient errors that are due to shards or nodes not being available. They are likely
	// due to Raft leader elections being in progress -e.g. a node was lost, or perhaps the quorum has been lost, or the
	// cluster is starting up.
	// In any case they should resolve once the election is complete, nodes replaced or cluster restarted. We don't want to timeout and
	// propagate errors in that case, we just wait until it is resolved. So we set this timeout to an effectively infinite
	// value
	operationTimeout = time.Duration(math.MaxInt64)
)

func init() {
	logger.SetLoggerFactory(logadaptor.LogrusLogFactory)
}

func NewDragon(cnf conf.Config, stopSignaller *common.AtomicBool) (*Dragon, error) {
	if len(cnf.RaftListenAddresses) < 3 {
		return nil, errors.Error("minimum cluster size is 3 nodes")
	}
	dragon := &Dragon{
		cnf:           cnf,
		shardSMs:      make(map[uint64]*ShardOnDiskStateMachine),
		stopSignaller: stopSignaller,
	}
	dragon.procMgr = newProcManager(dragon, cnf.RemotingListenAddresses)
	return dragon, nil
}

type Dragon struct {
	startStopLock                sync.Mutex
	nodeHostStarted              bool
	lock                         sync.RWMutex
	cnf                          conf.Config
	ingestDir                    string
	pebble                       *pebble.DB
	nh                           *dragonboat.NodeHost
	shardAllocs                  map[uint64][]int
	allDataShards                []uint64
	localDataShards              []uint64 // Note this is not local *leaders*, it is shards which have a replica locally - it is static
	started                      bool
	shardListenerFactory         cluster.ShardListenerFactory
	remoteQueryExecutionCallback cluster.RemoteQueryExecutionCallback
	shardSMsLock                 sync.Mutex
	shardSMs                     map[uint64]*ShardOnDiskStateMachine
	requestClient                *remoting.Client
	saveSnapshotCount            int64
	restoreSnapshotCount         int64
	procMgr                      *procManager
	stopSignaller                *common.AtomicBool
	forwardWriteHandler          cluster.ForwardWriteHandler
}

type snapshot struct {
	pebbleSnapshot *pebble.Snapshot
}

func (s *snapshot) Close() {
	if err := s.pebbleSnapshot.Close(); err != nil {
		log.Errorf("failed to close snapshot %+v", err)
	}
}

func (d *Dragon) RegisterShardListenerFactory(factory cluster.ShardListenerFactory) {
	d.shardListenerFactory = factory
}

func (d *Dragon) GetNodeID() int {
	return d.cnf.NodeID
}

func (d *Dragon) GenerateClusterSequence(sequenceName string) (uint64, error) {
	var buff []byte
	buff = common.AppendStringToBufferLE(buff, sequenceName)

	retVal, respBody, err := d.sendPropose(tableSequenceClusterID, buff, false)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	if retVal != seqStateMachineUpdatedOK {
		return 0, errors.Errorf("unexpected return value from writing sequence: %d", retVal)
	}
	seqVal, _ := common.ReadUint64FromBufferLE(respBody, 0)
	return seqVal, nil
}

func (d *Dragon) GetLock(prefix string) (bool, error) {
	return d.sendLockRequest(GetLockCommand, prefix)
}

func (d *Dragon) ReleaseLock(prefix string) (bool, error) {
	return d.sendLockRequest(ReleaseLockCommand, prefix)
}

func (d *Dragon) sendLockRequest(command string, prefix string) (bool, error) {
	var buff []byte
	buff = common.AppendStringToBufferLE(buff, command)
	buff = common.AppendStringToBufferLE(buff, prefix)
	// We generate a unique locker string - propose requests can be retried and the locks state machine must be
	// idempotent - if same lock request is retried it must succeed if current locker already has the lock
	locker := uuid.NewV4().String()
	buff = common.AppendStringToBufferLE(buff, locker)

	retVal, resBuff, err := d.sendPropose(locksClusterID, buff, false)
	if err != nil {
		return false, errors.WithStack(err)
	}
	if retVal != locksStateMachineUpdatedOK {
		return false, errors.Errorf("unexpected return value from lock request: %d", retVal)
	}
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
	return d.allDataShards
}

func (d *Dragon) GetLocalShardIDs() []uint64 {
	return d.localDataShards
}

func (d *Dragon) ExecuteRemotePullQuery(queryInfo *cluster.QueryExecutionInfo, rowsFactory *common.RowsFactory) (*common.Rows, error) {
	d.lock.RLock()
	defer d.lock.RUnlock()

	if !d.started {
		return nil, errors.Errorf("failed to execute query on node %d not started", d.cnf.NodeID)
	}

	if queryInfo.ShardID < cluster.DataShardIDBase {
		panic("invalid shard cluster id")
	}

	start := time.Now()
	var bytes []byte
	for {
		var buff []byte
		buff = append(buff, shardStateMachineLookupQuery)
		queryRequest, err := queryInfo.Serialize(buff)
		if err != nil {
			return nil, errors.WithStack(err)
		}

		bytes, err = d.executeRead(queryInfo.ShardID, queryRequest)

		if err != nil {
			err = errors.WithStack(errors.Errorf("failed to execute query on node %d %s %v", d.cnf.NodeID, queryInfo.Query, err))
			return nil, errors.WithStack(err)
		}

		if bytes[0] == 0 {
			if time.Now().Sub(start) > pullQueryRetryTimeout {
				msg := string(bytes[1:])
				return nil, errors.Errorf("failed to execute remote query %v", msg)
			}
			// Retry - the pull engine might not be fully started.... this can occur as the pull engine is not fully
			// initialised until after the cluster is active
			time.Sleep(500 * time.Millisecond)
		} else {
			break
		}
	}

	rows := rowsFactory.NewRows(1)
	rows.Deserialize(bytes[1:])
	return rows, nil
}

func (d *Dragon) LinearizableGet(shardID uint64, key []byte) ([]byte, error) {

	d.lock.RLock()
	defer d.lock.RUnlock()

	if !d.started {
		return nil, errors.Errorf("failed to execute query on node %d not started", d.cnf.NodeID)
	}

	if shardID < cluster.DataShardIDBase {
		panic("invalid shard cluster id")
	}

	start := time.Now()
	for {
		var buff []byte
		buff = append(buff, shardStateMachineLookupGet)
		buff = common.AppendUint32ToBufferLE(buff, uint32(len(key)))
		buff = append(buff, key...)

		result, err := d.executeRead(shardID, buff)

		if err != nil {
			err = errors.WithStack(errors.Errorf("failed to execute cluster get on node %d %v %v", d.cnf.NodeID, key, err))
			return nil, errors.WithStack(err)
		}

		if result[0] == 0 {
			if time.Now().Sub(start) > pullQueryRetryTimeout {
				msg := string(result[1:])
				return nil, errors.Errorf("failed to execute cluster get %v", msg)
			}
			// Retry - the pull engine might not be fully started.... this can occur as the pull engine is not fully
			// initialised until after the cluster is active
			time.Sleep(500 * time.Millisecond)
		} else {
			value := result[1:]
			if len(value) == 0 {
				return nil, nil
			}
			return value, nil
		}
	}
}

func (d *Dragon) SetRemoteQueryExecutionCallback(callback cluster.RemoteQueryExecutionCallback) {
	d.remoteQueryExecutionCallback = callback
}

// This encapsulates the first stage of start-up - after this point other nodes can contact the node, but groups may
// not have been joined yet
func (d *Dragon) start0() error {
	// We protect access with its own lock - remote read and write requests can come in before the Start() method is fully
	// complete
	d.lock.Lock()
	defer d.lock.Unlock()

	// Dragon logs a lot of non error stuff at error or warn - we screen these out (in tests mainly)
	if d.cnf.ScreenDragonLogSpam {
		logger.GetLogger("rsm").SetLevel(logger.ERROR)
		logger.GetLogger("transport").SetLevel(logger.ERROR)
		logger.GetLogger("grpc").SetLevel(logger.ERROR)
		logger.GetLogger("raft").SetLevel(logger.ERROR)
		logger.GetLogger("dragonboat").SetLevel(logger.ERROR)
	} else {
		logger.GetLogger("rsm").SetLevel(logger.INFO)
		logger.GetLogger("transport").SetLevel(logger.INFO)
		logger.GetLogger("grpc").SetLevel(logger.INFO)
		logger.GetLogger("raft").SetLevel(logger.DEBUG)
		logger.GetLogger("dragonboat").SetLevel(logger.DEBUG)
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

	// Currently the number of shards in the cluster is fixed. We must make sure the user doesn't change the number of shards
	// in the config after the cluster has been created. So we store the number in the database and check
	if err := d.checkConstantProperty("num-shards", d.cnf.NumShards); err != nil {
		return err
	}

	// Currently the replication factor of the cluster is fixed. We must make sure the user doesn't change the replication factor
	// in the config after the cluster has been created. So we store the number in the database and check
	if err := d.checkConstantProperty("replication-factor", d.cnf.ReplicationFactor); err != nil {
		return err
	}

	d.generateNodesAndShards(d.cnf.NumShards, d.cnf.ReplicationFactor)

	nodeAddress := d.cnf.RaftListenAddresses[d.cnf.NodeID]

	dragonBoatDir := filepath.Join(datadir, "dragon")

	nhc := config.NodeHostConfig{
		DeploymentID:      d.cnf.ClusterID,
		WALDir:            dragonBoatDir,
		NodeHostDir:       dragonBoatDir,
		RTTMillisecond:    uint64(d.cnf.RaftRTTMs),
		RaftAddress:       nodeAddress,
		EnableMetrics:     d.cnf.MetricsEnabled,
		Expert:            config.GetDefaultExpertConfig(),
		RaftEventListener: d.procMgr,
	}
	nhc.Expert.LogDB.EnableFsync = !d.cnf.DisableFsync

	nh, err := dragonboat.NewNodeHost(nhc)
	if err != nil {
		return errors.WithStack(err)
	}
	d.nh = nh
	d.nodeHostStarted = true
	return err
}

func (d *Dragon) Start() error { // nolint:gocyclo
	d.startStopLock.Lock()
	defer d.startStopLock.Unlock()

	if d.started {
		return nil
	}

	d.requestClient = &remoting.Client{}

	d.procMgr.Start()

	if err := d.start0(); err != nil {
		return err
	}

	log.Debugf("Joining groups on node %d", d.cnf.NodeID)

	err := d.joinSequenceGroup()
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

	d.started = true

	log.Infof("Prana node %d dragon started", d.cnf.NodeID)

	return nil
}

func (d *Dragon) executeSyncReadWithRetry(shardID uint64, request []byte) ([]byte, error) {
	res, err := d.executeRaftOpWithRetry(func() (interface{}, error) {
		ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
		defer cancel()
		res, err := d.nh.SyncRead(ctx, shardID, request)
		return res, errors.WithStack(err)
	})
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, nil
	}
	bytes, ok := res.([]byte)
	if !ok {
		panic("not a []byte")
	}
	return bytes, nil
}

func (d *Dragon) Stop() error {
	log.Debugf("Stopping dragon on node %d", d.cnf.NodeID)
	d.startStopLock.Lock()
	defer d.startStopLock.Unlock()
	d.lock.Lock()
	defer d.lock.Unlock()
	if !d.started {
		log.Debug("not started")
		return nil
	}
	d.procMgr.Stop()
	d.nh.Stop()
	d.requestClient.Stop()
	log.Debug("stopped node host")
	d.nh = nil
	d.nodeHostStarted = false
	err := d.pebble.Close()
	if err == nil {
		d.started = false
	}
	log.Debugf("Stopped Dragon node %d", d.cnf.NodeID)
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

func (d *Dragon) ExecuteForwardBatch(shardID uint64, batch []byte) error {
	// We handle this directly
	cs := d.nh.GetNoOPSession(shardID)
	res, err := d.proposeWithRetry(cs, batch)
	if err != nil {
		return err
	}
	if res.Value != shardStateMachineResponseOK {
		return errors.Errorf("unexpected return value from writing batch: %d to shard %d", res.Value,
			shardID)
	}
	return nil
}

func (d *Dragon) WriteForwardBatch(batch *cluster.WriteBatch, localOnly bool) error {
	if batch.ShardID < cluster.DataShardIDBase {
		panic(fmt.Sprintf("invalid shard cluster id %d", batch.ShardID))
	}
	var buff []byte
	buff = append(buff, shardStateMachineCommandForwardWrite)
	buff = batch.Serialize(buff)
	if localOnly {
		// For a local-only WriteForwardBatch we must not send the processor as this can cause deadlock if
		// the forward batch is being written from processing of another batch on the same processor
		// It is ok to send remotely, but as a propose - which bypasses the processor
		retVal, _, err := d.sendPropose(batch.ShardID, buff, false)
		if err != nil {
			return errors.WithStack(err)
		}
		if retVal != shardStateMachineResponseOK {
			return errors.Errorf("unexpected return value from writing batch: %d to shard %d", retVal,
				batch.ShardID)
		}
	} else {
		start := time.Now()
		for {
			leaderNodeID, ok := d.procMgr.getLeaderNode(batch.ShardID)
			var err error
			if ok {
				if leaderNodeID == uint64(d.cnf.NodeID) {
					err = d.forwardWriteHandler.HandleForwardWrite(batch.ShardID, buff)
				} else {
					request := &clustermsgs.ClusterForwardWriteRequest{
						ShardId:     int64(batch.ShardID),
						RequestBody: buff,
					}
					address := d.cnf.RemotingListenAddresses[leaderNodeID]
					_, err = d.requestClient.SendRPC(request, address)
				}
				if err == nil {
					return nil
				}
			}
			if err := d.retryLoopCheckError(start, err); err != nil {
				return err
			}
		}
	}
	return nil
}

func (d *Dragon) WriteBatch(batch *cluster.WriteBatch, localOnly bool) error {
	if batch.ShardID < cluster.DataShardIDBase {
		panic(fmt.Sprintf("invalid shard cluster id %d", batch.ShardID))
	}
	var buff []byte
	buff = append(buff, shardStateMachineCommandWrite)
	buff = batch.Serialize(buff)
	retval, _, err := d.sendPropose(batch.ShardID, buff, localOnly)
	if err != nil {
		return errors.WithStack(err)
	}
	if retval != shardStateMachineResponseOK {
		return errors.Errorf("unexpected return value from writing batch: %d to shard %d", retval, batch.ShardID)
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

func (d *Dragon) DeleteAllDataInRangeForShardLocally(shardID uint64, startPrefix []byte, endPrefix []byte) error {
	return d.deleteAllDataInRangeForShardsLocally(startPrefix, endPrefix, shardID)
}

func (d *Dragon) deleteAllDataInRangeForShardsLocally(startPrefix []byte, endPrefix []byte, shardIDs ...uint64) error {
	batch := d.pebble.NewBatch()
	for _, shardID := range shardIDs {
		// Remember, key must be in big-endian order
		startPrefixWithShard := make([]byte, 0, 16)
		startPrefixWithShard = common.AppendUint64ToBufferBE(startPrefixWithShard, shardID)
		startPrefixWithShard = append(startPrefixWithShard, startPrefix...)

		endPrefixWithShard := make([]byte, 0, 16)
		endPrefixWithShard = common.AppendUint64ToBufferBE(endPrefixWithShard, shardID)
		endPrefixWithShard = append(endPrefixWithShard, endPrefix...)

		if err := d.deleteAllDataInRangeLocally(batch, startPrefixWithShard, endPrefixWithShard); err != nil {
			return errors.WithStack(err)
		}
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

func (d *Dragon) DeleteAllDataInRangeForAllShardsLocally(startPrefix []byte, endPrefix []byte) error {
	return d.deleteAllDataInRangeForShardsLocally(startPrefix, endPrefix, d.localDataShards...)
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
	chans := make([]chan error, len(d.localDataShards))
	index := 0
	for _, shardID := range d.localDataShards {
		nodeIDs, ok := d.shardAllocs[shardID]
		if !ok {
			panic("cannot find shard alloc")
		}
		ch := make(chan error)
		// We join them in parallel
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
		ElectionRTT:        uint64(d.cnf.RaftElectionRTT),
		HeartbeatRTT:       uint64(d.cnf.RaftHeartbeatRTT),
		CheckQuorum:        true,
		SnapshotEntries:    uint64(d.cnf.DataSnapshotEntries),
		CompactionOverhead: uint64(d.cnf.DataCompactionOverhead),
		ClusterID:          shardID,
	}

	initialMembers := make(map[uint64]string)
	for _, nodeID := range nodeIDs {
		initialMembers[uint64(nodeID+1)] = d.cnf.RaftListenAddresses[nodeID]
	}

	createSMFunc := func(_ uint64, _ uint64) statemachine.IOnDiskStateMachine {
		return newShardODStateMachine(d, shardID, d.cnf.NodeID, nodeIDs)
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
		ElectionRTT:        uint64(d.cnf.RaftElectionRTT),
		HeartbeatRTT:       uint64(d.cnf.RaftHeartbeatRTT),
		CheckQuorum:        true,
		SnapshotEntries:    uint64(d.cnf.SequenceSnapshotEntries),
		CompactionOverhead: uint64(d.cnf.SequenceCompactionOverhead),
		ClusterID:          tableSequenceClusterID,
	}

	nodeIDs, ok := d.shardAllocs[tableSequenceClusterID]
	if !ok {
		panic("can't find table sequence shard allocs")
	}
	initialMembers := make(map[uint64]string)
	thisNode := false
	for _, nodeID := range nodeIDs {
		initialMembers[uint64(nodeID+1)] = d.cnf.RaftListenAddresses[nodeID]
		if nodeID == d.cnf.NodeID {
			thisNode = true
		}
	}

	if thisNode {
		if err := d.nh.StartOnDiskCluster(initialMembers, false, d.newSequenceODStateMachine, rc); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func (d *Dragon) joinLockGroup() error {
	rc := config.Config{
		NodeID:             uint64(d.cnf.NodeID + 1),
		ElectionRTT:        uint64(d.cnf.RaftElectionRTT),
		HeartbeatRTT:       uint64(d.cnf.RaftHeartbeatRTT),
		CheckQuorum:        true,
		SnapshotEntries:    uint64(d.cnf.LocksSnapshotEntries),
		CompactionOverhead: uint64(d.cnf.LocksCompactionOverhead),
		ClusterID:          locksClusterID,
	}

	nodeIDs, ok := d.shardAllocs[locksClusterID]
	if !ok {
		panic("can't find locks shard allocs")
	}
	initialMembers := make(map[uint64]string)
	thisNode := false
	for _, nodeID := range nodeIDs {
		initialMembers[uint64(nodeID+1)] = d.cnf.RaftListenAddresses[nodeID]
		if nodeID == d.cnf.NodeID {
			thisNode = true
		}
	}

	if thisNode {
		if err := d.nh.StartOnDiskCluster(initialMembers, false, d.newLocksODStateMachine, rc); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

// For now, the number of shards and nodes in the cluster is static so we can just calculate this on startup
// on each node
func (d *Dragon) generateNodesAndShards(numShards int, replicationFactor int) {
	numNodes := len(d.cnf.RaftListenAddresses)
	allNodes := make([]int, numNodes)
	for i := 0; i < numNodes; i++ {
		allNodes[i] = i
	}
	d.localDataShards = make([]uint64, 0)
	d.allDataShards = make([]uint64, numShards)
	d.shardAllocs = make(map[uint64][]int)
	for i := 0; i < numShards; i++ {
		shardID := cluster.DataShardIDBase + uint64(i)
		d.allDataShards[i] = shardID
		d.generateReplicas(shardID, replicationFactor, true)
	}
	d.generateReplicas(tableSequenceClusterID, replicationFactor, false)
	d.generateReplicas(locksClusterID, replicationFactor, false)
}

func (d *Dragon) generateReplicas(shardID uint64, replicationFactor int, dataShard bool) {
	numNodes := len(d.cnf.RaftListenAddresses)
	nids := make([]int, replicationFactor)
	for j := 0; j < replicationFactor; j++ {
		nids[j] = (int(shardID + uint64(j))) % numNodes
		if nids[j] == d.cnf.NodeID {
			if dataShard {
				d.localDataShards = append(d.localDataShards, shardID)
			}
		}
	}
	d.shardAllocs[shardID] = nids
}

// Some errors are expected, we should retry in this case
// See https://github.com/lni/dragonboat/issues/183
func (d *Dragon) executeRaftOpWithRetry(f func() (interface{}, error)) (interface{}, error) {
	start := time.Now()
	errTimeoutCount := 0
	for {
		res, err := f()
		if err == nil {
			return res, nil
		}
		// These errors are to be expected and transient
		// ErrClusterNotReady if there is no leader - e.g. election in progress or no quorum (node lost or startup)
		// ErrTimeout - can occur if the operation takes too long to process - e.g. when under load
		// The others - typically when the cluster is still starting up or closing down and the raft groups are
		// We retry in this case
		if !errors.Is(err, dragonboat.ErrClusterNotReady) && !errors.Is(err, dragonboat.ErrTimeout) &&
			!errors.Is(err, dragonboat.ErrClusterNotFound) && !errors.Is(err, dragonboat.ErrClusterClosed) &&
			!errors.Is(err, dragonboat.ErrClusterNotInitialized) && !errors.Is(err, dragonboat.ErrClusterNotBootstrapped) &&
			!errors.Is(err, dragonboat.ErrInvalidDeadline) {
			return nil, errors.WithStack(err)
		}
		if time.Now().Sub(start) >= operationTimeout {
			// If we timeout, then something is seriously wrong
			log.Errorf("error in making dragonboat calls %+v", err)
			return nil, err
		}
		if d.stopSignaller.Get() {
			return nil, errors.New("server is closing")
		}
		var delay time.Duration
		if err == dragonboat.ErrTimeout {
			// For Raft timeouts we backoff longer before retrying
			errTimeoutCount++
			delay = callTimeout
			if errTimeoutCount > 5 {
				log.Warnf("errTimeout retry count is %d", errTimeoutCount)
			}
		} else {
			delay = 500 * time.Millisecond
		}
		// Randomise the delay to prevent clashing concurrent retries
		randDelay := float64(delay) + 0.5*float64(delay)*(rand.Float64()-0.5)
		time.Sleep(time.Duration(randDelay))
	}
}

func (d *Dragon) proposeWithRetry(session *client.Session, cmd []byte) (statemachine.Result, error) {
	r, err := d.executeRaftOpWithRetry(func() (interface{}, error) {
		ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
		defer cancel()
		res, err := d.nh.SyncPropose(ctx, session, cmd)
		return res, errors.WithStack(err)
	})
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
		key := createToDeleteKey(deleteBatch.ConditionalTableID, prefix)
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
		key := createToDeleteKey(deleteBatch.ConditionalTableID, prefix)
		if err := batch.Delete(key, nil); err != nil {
			return errors.WithStack(err)
		}
	}
	if err := d.pebble.Apply(batch, nosyncWriteOptions); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func createToDeleteKey(conditionalTableID uint64, prefix []byte) []byte {
	key := table.EncodeTableKeyPrefix(common.ToDeleteTableID, toDeleteShardID, 16+1+8+len(prefix))
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
	var currBatch *cluster.ToDeleteBatch

	log.Debugf("There are %d to_delete entries", len(kvPairs))

	for _, kvPair := range kvPairs {
		offset := 16
		conditionalTableID, _ := common.ReadUint64FromBufferBE(kvPair.Key, offset)
		offset += 8
		prefix := kvPair.Key[offset:]

		if currBatch == nil || currBatch.ConditionalTableID != conditionalTableID {
			currBatch = &cluster.ToDeleteBatch{
				ConditionalTableID: conditionalTableID,
			}
			localBatches = append(localBatches, currBatch)
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

	// Now remove from to_delete table
	for _, batch := range localBatches {
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

func (d *Dragon) GetConfigProperty(property string) ([]byte, error) {
	key := table.EncodeTableKeyPrefix(common.LocalConfigTableID, 0, 16)
	propKey := []byte(property)
	key = common.AppendUint32ToBufferBE(key, uint32(len(propKey)))
	key = append(key, propKey...)
	return d.LocalGet(key)
}

func (d *Dragon) SetConfigProperty(property string, v []byte) error {
	key := table.EncodeTableKeyPrefix(common.LocalConfigTableID, 0, 16)
	propKey := []byte(property)
	key = common.AppendUint32ToBufferBE(key, uint32(len(propKey)))
	key = append(key, propKey...)
	batch := d.pebble.NewBatch()
	if err := batch.Set(key, v, nil); err != nil {
		return errors.WithStack(err)
	}
	if err := d.pebble.Apply(batch, syncWriteOptions); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (d *Dragon) checkConstantProperty(property string, expectedValue int) error {
	log.Debugf("Checking constant %s: %d", property, expectedValue)
	v, err := d.GetConfigProperty(property)
	if err != nil {
		return err
	}
	if v == nil {
		log.Debugf("New cluster - no value for %s in storage, persisting it", property)
		v = common.AppendUint32ToBufferLE([]byte{}, uint32(expectedValue))
		err = d.SetConfigProperty(property, v)
		if err != nil {
			return err
		}
	} else {
		value, _ := common.ReadUint32FromBufferLE(v, 0)
		log.Debugf("value for %s found in storage: %d", property, value)
		if int(value) != expectedValue {
			return errors.Errorf("%s in cluster cannot be changed after cluster creation. cluster value %d new value %d", property, expectedValue, value)
		}
	}
	return nil
}

func (d *Dragon) registerShardSM(shardID uint64, sm *ShardOnDiskStateMachine) {
	d.shardSMsLock.Lock()
	defer d.shardSMsLock.Unlock()
	if _, ok := d.shardSMs[shardID]; ok {
		panic(fmt.Sprintf("Shard SM for shard %d already registered on node %d", shardID, d.cnf.NodeID))
	}
	d.shardSMs[shardID] = sm
}

func (d *Dragon) unregisterShardSM(shardID uint64) {
	d.shardSMsLock.Lock()
	defer d.shardSMsLock.Unlock()
	delete(d.shardSMs, shardID)
}

func (d *Dragon) sendPropose(shardID uint64, request []byte, localOnly bool) (uint64, []byte, error) {
	start := time.Now()
	for {
		leaderNodeID, ok := d.procMgr.getLeaderNode(shardID)
		var err error
		if ok {
			if d.cnf.TestServer || leaderNodeID == uint64(d.cnf.NodeID) {
				// We handle this directly
				cs := d.nh.GetNoOPSession(shardID)
				res, err := d.proposeWithRetry(cs, request)
				return res.Value, res.Data, err
			}
			if localOnly {
				return 0, nil, errors.Errorf("no local leader for shard %d (localOnly=true)", shardID)
			}
			// Send remotely
			address := d.cnf.RemotingListenAddresses[leaderNodeID]
			clusterRequest := &clustermsgs.ClusterProposeRequest{
				ShardId:     int64(shardID),
				RequestBody: request,
			}
			var resp remoting.ClusterMessage
			resp, err = d.requestClient.SendRPC(clusterRequest, address)
			if err == nil {
				clusterResp, ok := resp.(*clustermsgs.ClusterProposeResponse)
				if !ok {
					panic("not a clustermsgs.ClusterReadResponse")
				}
				return uint64(clusterResp.RetVal), clusterResp.GetResponseBody(), nil
			}
		}
		if err := d.retryLoopCheckError(start, err); err != nil {
			return 0, nil, err
		}
	}
}

func (d *Dragon) executeRead(shardID uint64, request []byte) ([]byte, error) {
	start := time.Now()
	for {
		leaderNodeID, ok := d.procMgr.getLeaderNode(shardID)
		var err error
		if ok {
			if leaderNodeID == uint64(d.cnf.NodeID) {
				// We handle this directly
				return d.executeSyncReadWithRetry(shardID, request)
			}
			// Send remotely
			address := d.cnf.RemotingListenAddresses[leaderNodeID]
			clusterRequest := &clustermsgs.ClusterReadRequest{
				ShardId:     int64(shardID),
				RequestBody: request,
			}
			var resp remoting.ClusterMessage
			resp, err = d.requestClient.SendRPC(clusterRequest, address)
			if err == nil {
				clusterResp, ok := resp.(*clustermsgs.ClusterReadResponse)
				if !ok {
					panic("not a clustermsgs.ClusterReadResponse")
				}
				return clusterResp.GetResponseBody(), nil
			}
		}
		if err := d.retryLoopCheckError(start, err); err != nil {
			return nil, err
		}
	}
}

func (d *Dragon) retryLoopCheckError(start time.Time, err error) error {
	if err != nil {
		var perr errors.PranaError
		if !(errors.As(err, &perr) && perr.Code == errors.Unavailable) {
			log.Debugf("not an unavailable err so not retrying %v", err)
			return err
		}
	}
	if d.stopSignaller.Get() {
		// Server is closing
		return errors.New("server is closing")
	}
	// Retry
	time.Sleep(1 * time.Second)
	if time.Now().Sub(start) >= operationTimeout {
		return errors.NewPranaErrorf(errors.Timeout, "timed out sending rpc to leader")
	}
	return nil
}

func (d *Dragon) GetRemoteProposeHandler() remoting.ClusterMessageHandler {
	return &proposeHandler{d: d}
}

type proposeHandler struct {
	d *Dragon
}

func (p *proposeHandler) HandleMessage(clusterMsg remoting.ClusterMessage) (remoting.ClusterMessage, error) {
	req, ok := clusterMsg.(*clustermsgs.ClusterProposeRequest)
	if !ok {
		panic(fmt.Sprintf("not a *clustermsgs.ClusterProposeRequest %v", req))
	}
	return p.d.handleRemoteProposeRequest(req)
}

func (d *Dragon) GetRemoteReadHandler() remoting.ClusterMessageHandler {
	return &readHandler{d: d}
}

func (d *Dragon) handleRemoteProposeRequest(request *clustermsgs.ClusterProposeRequest) (*clustermsgs.ClusterProposeResponse, error) {
	d.lock.RLock()
	defer d.lock.RUnlock()

	if err := d.ensureNodeHostAvailable(); err != nil {
		return nil, err
	}
	cs := d.nh.GetNoOPSession(uint64(request.ShardId))
	res, err := d.proposeWithRetry(cs, request.RequestBody)
	if err != nil {
		return nil, err
	}
	resp := &clustermsgs.ClusterProposeResponse{
		RetVal:       int64(res.Value),
		ResponseBody: res.Data,
	}
	return resp, nil
}

type readHandler struct {
	d *Dragon
}

func (p *readHandler) HandleMessage(clusterMsg remoting.ClusterMessage) (remoting.ClusterMessage, error) {
	req, ok := clusterMsg.(*clustermsgs.ClusterReadRequest)
	if !ok {
		panic("not a *clustermsgs.ClusterReadRequest")
	}
	return p.d.handleRemoteReadRequest(req)
}

func (d *Dragon) handleRemoteReadRequest(request *clustermsgs.ClusterReadRequest) (*clustermsgs.ClusterReadResponse, error) {
	d.lock.RLock()
	defer d.lock.RUnlock()
	if err := d.ensureNodeHostAvailable(); err != nil {
		return nil, err
	}
	respBody, err := d.executeSyncReadWithRetry(uint64(request.ShardId), request.RequestBody)
	if err != nil {
		return nil, err
	}
	resp := &clustermsgs.ClusterReadResponse{
		ResponseBody: respBody,
	}
	return resp, nil
}

type leaderInfoHandler struct {
	d *Dragon
}

func (d *Dragon) GetLeaderInfosHandler() remoting.ClusterMessageHandler {
	return &leaderInfoHandler{d: d}
}

func (l *leaderInfoHandler) HandleMessage(clusterMsg remoting.ClusterMessage) (remoting.ClusterMessage, error) {
	msg, ok := clusterMsg.(*clustermsgs.LeaderInfosMessage)
	if !ok {
		panic("not a *clustermsgs.LeaderInfosMessage")
	}
	l.d.procMgr.handleLeaderInfosMessage(msg)
	return nil, nil
}

// Waits until the node host is available - is called with the lock RLock held and always returns with it held
func (d *Dragon) ensureNodeHostAvailable() error {
	if d.nodeHostStarted {
		return nil
	}
	d.lock.RUnlock()
	start := time.Now()
	for {
		time.Sleep(100 * time.Millisecond)
		d.lock.RLock()
		if time.Now().Sub(start) > nodeHostStartTimeout {
			return errors.New("timed out waiting for nodehost to start")
		}
		if d.nodeHostStarted {
			break
		}
		d.lock.RUnlock()
	}
	return nil
}

func (d *Dragon) SaveSnapshotCount() int64 {
	return atomic.LoadInt64(&d.saveSnapshotCount)
}

func (d *Dragon) RestoreSnapshotCount() int64 {
	return atomic.LoadInt64(&d.restoreSnapshotCount)
}

func (d *Dragon) SyncStore() error {
	d.lock.RLock()
	defer d.lock.RUnlock()
	if !d.started {
		return nil
	}
	return syncPebble(d.pebble)
}

func (d *Dragon) setLeader(shardID uint64, term uint64) error {
	var buff []byte
	buff = append(buff, shardStateMachineCommandSetLeader)
	buff = common.AppendUint64ToBufferLE(buff, uint64(d.cnf.NodeID))
	buff = common.AppendUint64ToBufferLE(buff, term)
	cs := d.nh.GetNoOPSession(shardID)
	res, err := d.proposeWithRetry(cs, buff)
	if err != nil {
		return err
	}
	if res.Value != shardStateMachineResponseOK {
		return errors.Errorf("unexpected return value from setLeader %d", res.Value)
	}
	return nil
}

func (d *Dragon) GetLeadersMap() (map[uint64]uint64, error) {
	leadersMap := d.procMgr.getLeadersMap()
	// We need to verify we have leaders for all data shards
	for _, shardID := range d.allDataShards {
		_, ok := leadersMap[shardID]
		if !ok {
			return nil, errors.NewPranaErrorf(errors.DdlRetry, "not all shards have leaders")
		}
	}
	return leadersMap, nil
}

func (d *Dragon) RegisterStartFill(expectedLeaders map[uint64]uint64, interruptor *interruptor.Interruptor) error {
	return d.procMgr.registerStartFill(expectedLeaders, interruptor)
}

func (d *Dragon) RegisterEndFill() {
	d.procMgr.registerEndFill()
}

func (d *Dragon) SetForwardWriteHandler(forwardWriteHandler cluster.ForwardWriteHandler) {
	d.forwardWriteHandler = forwardWriteHandler
}

func (d *Dragon) TableDropped(tableID uint64) {
	d.shardSMsLock.Lock()
	defer d.shardSMsLock.Unlock()
	for _, sm := range d.shardSMs {
		sm.tableDropped(tableID)
	}
}
