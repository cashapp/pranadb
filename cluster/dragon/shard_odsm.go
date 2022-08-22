package dragon

import (
	"fmt"
	"github.com/cockroachdb/pebble"
	"github.com/hashicorp/golang-lru/simplelru"
	"github.com/lni/dragonboat/v3/statemachine"
	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/table"
	"io"
	"math"
	"sync"
	"sync/atomic"
)

const (
	shardStateMachineLookupPing          byte   = 1
	shardStateMachineLookupQuery         byte   = 2
	shardStateMachineLookupGet           byte   = 3
	shardStateMachineCommandWrite        byte   = 1
	shardStateMachineCommandForwardWrite byte   = 2
	shardStateMachineCommandSetLeader    byte   = 3
	shardStateMachineResponseOK          uint64 = 1
	droppedTableCacheMaxSize                    = 100
)

func newShardODStateMachine(d *Dragon, shardID uint64, nodeID int, nodeIDs []int) *ShardOnDiskStateMachine {
	ssm := ShardOnDiskStateMachine{
		nodeID:  nodeID,
		nodeIDs: nodeIDs,
		shardID: shardID,
		dragon:  d,
	}
	return &ssm
}

type ShardOnDiskStateMachine struct {
	nodeID           int
	shardID          uint64
	dragon           *Dragon
	nodeIDs          []int
	shardListener    cluster.ShardListener
	dedupSequences   map[string]uint64
	receiverSequence uint64
	forwardRows      []cluster.ForwardRow
	lock             sync.Mutex
	lastIndex        uint64
	leaderNodeID     int64
	leaderTerm       int64
	droppedTables    *simplelru.LRU
}

func (s *ShardOnDiskStateMachine) Open(stopc <-chan struct{}) (uint64, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.dragon.registerShardSM(s.shardID, s)
	if err := s.loadDedupCache(); err != nil {
		return 0, err
	}
	lastRaftIndex, receiverSequence, err := s.loadSequences(s.dragon.pebble, s.shardID)
	if err != nil {
		return 0, err
	}
	s.receiverSequence = receiverSequence
	leaderNodeID, term, err := s.loadLeaderInfo(s.dragon.pebble, s.shardID)
	if err != nil {
		return 0, err
	}
	s.leaderNodeID = leaderNodeID
	s.leaderTerm = term
	// We maintain a small cache of dropped table ids with a bounded upper size
	dt, err := simplelru.NewLRU(droppedTableCacheMaxSize, nil)
	if err != nil {
		return 0, err
	}
	s.droppedTables = dt
	return lastRaftIndex, nil
}

func (s *ShardOnDiskStateMachine) loadLeaderInfo(peb *pebble.DB, shardID uint64) (int64, int64, error) {
	key := table.EncodeTableKeyPrefix(common.ShardLeaderTableID, shardID, 16)
	vb, closer, err := peb.Get(key)
	defer common.InvokeCloser(closer)
	if err == pebble.ErrNotFound {
		return -1, -1, nil
	}
	if err != nil {
		return 0, 0, errors.WithStack(err)
	}
	nodeID, _ := common.ReadUint64FromBufferLE(vb, 0)
	term, _ := common.ReadUint64FromBufferLE(vb, 8)
	return int64(nodeID), int64(term), nil
}

func (s *ShardOnDiskStateMachine) loadSequences(peb *pebble.DB, shardID uint64) (uint64, uint64, error) {
	// read the index of the last persisted log entry and the last written receiver sequence
	key := table.EncodeTableKeyPrefix(common.LastLogIndexReceivedTableID, shardID, 16)
	vb, closer, err := peb.Get(key)
	defer common.InvokeCloser(closer)
	if err == pebble.ErrNotFound {
		return 0, 0, nil
	}
	if err != nil {
		return 0, 0, errors.WithStack(err)
	}
	lastRaftIndex, _ := common.ReadUint64FromBufferLE(vb, 0)
	receiverSequence, _ := common.ReadUint64FromBufferLE(vb, 8)
	return lastRaftIndex, receiverSequence, nil
}

func (s *ShardOnDiskStateMachine) writeSequences(batch *pebble.Batch, lastRaftIndex uint64,
	receiverSequence uint64, shardID uint64) error {
	// We store the last received and persisted log entry and the last written receiver sequence
	key := table.EncodeTableKeyPrefix(common.LastLogIndexReceivedTableID, shardID, 16)
	vb := make([]byte, 0, 16)
	vb = common.AppendUint64ToBufferLE(vb, lastRaftIndex)
	vb = common.AppendUint64ToBufferLE(vb, receiverSequence)
	return batch.Set(key, vb, nil)
}

func (s *ShardOnDiskStateMachine) isProcessor() bool {
	return int64(s.nodeID) == s.leaderNodeID
}

func (s *ShardOnDiskStateMachine) Update(entries []statemachine.Entry) ([]statemachine.Entry, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.lastIndex != 0 && s.lastIndex != entries[0].Index-1 {
		log.Warnf("shard state machine %d entries are being replayed expected %d got %d", s.shardID,
			s.lastIndex+1, entries[0].Index)
		s.lastIndex = entries[len(entries)-1].Index
	}
	batch := s.dragon.pebble.NewBatch()
	timestamp := common.NanoTime()
	for i, entry := range entries {
		cmdBytes := entry.Cmd
		command := cmdBytes[0]
		switch command {
		case shardStateMachineCommandForwardWrite:
			if s.forwardRows == nil && s.isProcessor() {
				// Most likely the entries will be all forward writes
				s.forwardRows = make([]cluster.ForwardRow, 0, len(entries))
			}
			if err := s.handleWrite(batch, cmdBytes, true, timestamp); err != nil {
				return nil, err
			}
		case shardStateMachineCommandWrite:
			if err := s.handleWrite(batch, cmdBytes, false, timestamp); err != nil {
				return nil, errors.WithStack(err)
			}
		case shardStateMachineCommandSetLeader:
			if err := s.handleSetLeader(batch, cmdBytes); err != nil {
				return nil, errors.WithStack(err)
			}
		default:
			panic(fmt.Sprintf("unexpected command %d", command))
		}
		entries[i].Result = statemachine.Result{Value: shardStateMachineResponseOK}
	}
	lastLogIndex := entries[len(entries)-1].Index

	// We store the last received and persisted log entry
	key := table.EncodeTableKeyPrefix(common.LastLogIndexReceivedTableID, s.shardID, 16)
	vb := make([]byte, 0, 8)
	common.AppendUint64ToBufferLE(vb, lastLogIndex)
	if err := batch.Set(key, vb, nil); err != nil {
		return nil, errors.WithStack(err)
	}
	if err := s.writeSequences(batch, lastLogIndex, s.receiverSequence, s.shardID); err != nil {
		return nil, errors.WithStack(err)
	}

	if err := s.dragon.pebble.Apply(batch, nosyncWriteOptions); err != nil {
		return nil, errors.WithStack(err)
	}

	// A forward write is a write which forwards a batch of rows from one shard to another
	// In this case we want to trigger processing of those rows, if we're the processor
	if s.shardListener != nil && len(s.forwardRows) > 0 {
		s.shardListener.RemoteWriteOccurred(s.forwardRows)
		s.forwardRows = nil
	}
	return entries, nil
}

func (s *ShardOnDiskStateMachine) handleWrite(batch *pebble.Batch, bytes []byte, forward bool, timestamp uint64) error {
	puts, deletes := s.deserializeWriteBatch(bytes, 1, forward)
	hasDups := false
	for _, kvPair := range puts {

		var key []byte
		if forward {

			dedupKey := kvPair.Key[:24] // Next 24 bytes is the dedup key

			ignore, err := s.checkDedup(dedupKey, batch)
			if err != nil {
				return err
			}
			if ignore {
				hasDups = true
				continue
			}

			remoteConsumerBytes := kvPair.Key[24:] // The rest is just the remote consumer id

			// We increment before using - receiver sequence must start at 1
			s.receiverSequence++

			// For a write into the receiver table (forward write) the key is constructed as follows:
			// shard_id|receiver_table_id|receiver_sequence|remote_consumer_id
			key = table.EncodeTableKeyPrefix(common.ReceiverTableID, s.shardID, 40)
			key = common.AppendUint64ToBufferBE(key, s.receiverSequence)
			key = append(key, remoteConsumerBytes...)

			if s.isProcessor() {
				remoteConsumerID, _ := common.ReadUint64FromBufferBE(remoteConsumerBytes, 0)
				s.forwardRows = append(s.forwardRows, cluster.ForwardRow{
					ReceiverSequence: s.receiverSequence,
					RemoteConsumerID: remoteConsumerID,
					KeyBytes:         key,
					RowBytes:         kvPair.Value,
					WriteTime:        timestamp,
				})
			}

		} else {
			key = kvPair.Key
			if !s.checkKey(key) {
				log.Debugf("not writing data key %v as table is dropped", key)
				continue
			}
		}
		if err := batch.Set(key, kvPair.Value, nil); err != nil {
			return errors.WithStack(err)
		}
	}
	if forward && len(deletes) != 0 {
		panic("deletes not supported for forward write")
	}
	for _, k := range deletes {
		s.checkKey(k)
		if err := batch.Delete(k, nil); err != nil {
			return errors.WithStack(err)
		}
	}
	if hasDups {
		log.Warnf("Write batch in shard %d - contained duplicates - these were screened out", s.shardID)
	}
	return nil
}

// We deserialize into simple slices for puts and deletes as we don't need the actual WriteBatch instance in the
// state machine
func (s *ShardOnDiskStateMachine) deserializeWriteBatch(buff []byte, offset int, forward bool) (puts []cluster.KVPair, deletes [][]byte) {
	var numPuts, numDeletes uint32
	numPuts, offset = common.ReadUint32FromBufferLE(buff, offset)
	numDeletes, offset = common.ReadUint32FromBufferLE(buff, offset)
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
		var kToUse []byte
		if forward {
			kCopy := common.CopyByteSlice(k)
			kCopy = common.AppendUint64ToBufferBE(kCopy, s.receiverSequence)
			kToUse = kCopy
		} else {
			kToUse = k
		}
		puts[i] = cluster.KVPair{
			Key:   kToUse,
			Value: v,
		}
	}

	deletes = make([][]byte, numDeletes)
	for i := 0; i < int(numDeletes); i++ {
		var kl uint32
		kl, offset = common.ReadUint32FromBufferLE(buff, offset)
		kLen := int(kl)
		k := buff[offset : offset+kLen]
		offset += kLen
		deletes[i] = k
	}
	return puts, deletes
}

func (s *ShardOnDiskStateMachine) checkDedup(key []byte, batch *pebble.Batch) (ignore bool, err error) {
	ignore, err = cluster.DoDedup(s.shardID, key, s.dedupSequences)
	if err != nil {
		return false, err
	}
	if ignore {
		return true, nil
	}

	// Persist the duplicate entry
	dupID := table.EncodeTableKeyPrefix(common.ForwardDedupTableID, s.shardID, 16+8)
	dupID = append(dupID, key[:16]...) // Originator id
	// The value is the sequence
	if err := batch.Set(dupID, key[16:], nil); err != nil {
		return false, errors.WithStack(err)
	}
	return false, nil
}

func (s *ShardOnDiskStateMachine) checkKey(key []byte) bool {
	if s.dragon.cnf.TestServer {
		return true
	}
	// Sanity check
	sid, _ := common.ReadUint64FromBufferBE(key, 0)
	if s.shardID != sid {
		panic(fmt.Sprintf("invalid key in sm write, expected %d actual %d", s.shardID, sid))
	}
	tableID, _ := common.ReadUint64FromBufferBE(key, 8)
	_, ok := s.droppedTables.Get(tableID)
	if ok {
		// Table has been dropped we ignore the write - this is important as perform drop data locally so we could
		// otherwise read replicated writes on a slow follower after a table has been dropped leaving orphaned rows
		// in the database.
		// It does mean each state machine replica can diverge slightly but it does not matter because the divergence
		// is only with dropped table data and we will never look at dropped table data again in Raft.
		return false
	}
	return true
}

func (s *ShardOnDiskStateMachine) handleSetLeader(batch *pebble.Batch, bytes []byte) error {

	newLeaderNodeID, _ := common.ReadUint64FromBufferLE(bytes, 1)
	term, _ := common.ReadUint64FromBufferLE(bytes, 9)
	if int64(term) <= s.leaderTerm {
		// Logs can be replayed and the SM needs to be idempotent, we could also have competing setLeader attempts from
		// different terms executed in wrong order (theoretically). In either case ignore
		return nil
	}
	// Persist the leader node id and term
	key := table.EncodeTableKeyPrefix(common.ShardLeaderTableID, s.shardID, 16)
	var value []byte
	value = common.AppendUint64ToBufferLE(value, newLeaderNodeID)
	value = common.AppendUint64ToBufferLE(value, term)
	if err := batch.Set(key, value, nil); err != nil {
		return err
	}

	prevLeaderNodeID := s.leaderNodeID
	s.leaderNodeID = int64(newLeaderNodeID)
	s.leaderTerm = int64(term)

	if thisNodeID := uint64(s.nodeID); newLeaderNodeID == thisNodeID {
		// Please note that the concept of "leader" as set here is not strictly in step with the actual Raft leader
		// When actual raft leadership changes there is a window before setLeader is called where the actual raft leader
		// and what this group thinks is the raft leader are different. But that does not matter. What matters is that
		// all replicas agree on who the "leader" is and that most of the time it's the same as the actual Raft leader
		// (for performance reasons). The key thing is that processing only occurs on one replica of the cluster at any one
		// time.
		if s.shardListener == nil {
			s.shardListener = s.dragon.shardListenerFactory.CreateShardListener(s.shardID)
		}
	} else if newLeaderNodeID != thisNodeID && prevLeaderNodeID == int64(thisNodeID) {
		// We were leader but not any more
		if s.shardListener != nil {
			// Shard listener could be nil, if this occurs right after startup
			s.shardListener.Close()
			s.shardListener = nil
		}
	}
	return nil
}

func (s *ShardOnDiskStateMachine) Lookup(i interface{}) (interface{}, error) {
	defer common.PanicHandler()
	buff, ok := i.([]byte)
	if !ok {
		panic("expected []byte")
	}
	if typ := buff[0]; typ == shardStateMachineLookupPing {
		// A ping
		return nil, nil
	} else if typ == shardStateMachineLookupQuery {
		queryInfo := &cluster.QueryExecutionInfo{}
		err := queryInfo.Deserialize(buff[1:])
		if err != nil {
			return nil, errors.WithStack(err)
		}
		rows, err := s.dragon.remoteQueryExecutionCallback.ExecuteRemotePullQuery(queryInfo)
		if err != nil {
			return encodeError(err), nil
		}
		b := rows.Serialize()
		res := make([]byte, 0, 1+len(b))
		res = append(res, 1) // 1 signifies no error
		res = append(res, b...)
		return res, nil
	} else if typ == shardStateMachineLookupGet {
		keyLen, _ := common.ReadUint32FromBufferLE(buff, 1)
		key := buff[5 : 5+keyLen]
		val, err := s.dragon.LocalGet(key)
		if err != nil {
			return encodeError(err), nil
		}
		res := make([]byte, 0, 1+len(val))
		res = append(res, 1)
		res = append(res, val...)
		return res, nil
	} else {
		panic("invalid lookup type")
	}
}

func encodeError(err error) []byte {
	var buff []byte
	buff = append(buff, 0) // Zero byte signifies error
	buff = append(buff, err.Error()...)
	// Note - we don't send back an error to Dragonboat if a query failed - we only return an error
	// for an unrecoverable error.
	return buff
}

func (s *ShardOnDiskStateMachine) Sync() error {
	if s.dragon.cnf.DisableFsync {
		return syncPebble(s.dragon.pebble)
	}
	return nil
}

func (s *ShardOnDiskStateMachine) PrepareSnapshot() (interface{}, error) {
	snapshot := s.dragon.pebble.NewSnapshot()
	return snapshot, nil
}

func (s *ShardOnDiskStateMachine) SaveSnapshot(i interface{}, writer io.Writer, _ <-chan struct{}) error {
	log.Debugf("data shard %d saving snapshot on node id %d", s.shardID, s.dragon.cnf.NodeID)
	snapshot, ok := i.(*pebble.Snapshot)
	if !ok {
		panic("not a snapshot")
	}
	prefix := make([]byte, 0, 8)
	prefix = common.AppendUint64ToBufferBE(prefix, s.shardID)
	log.Debugf("Saving data snapshot on node id %d for shard id %d prefix is %v", s.dragon.cnf.NodeID, s.shardID, prefix)
	err := saveSnapshotDataToWriter(s.dragon.pebble, snapshot, prefix, writer, s.shardID)
	atomic.AddInt64(&s.dragon.saveSnapshotCount, 1)
	if err != nil {
		// According to the docs for IOnDiskStateMachine we should return ErrSnapshotStreaming
		if errors.Is(err, statemachine.ErrSnapshotStreaming) {
			return err
		}
		log.Errorf("failure in streaming snapshot %+v", err)
		return statemachine.ErrSnapshotStreaming
	}
	log.Debugf("data shard %d save snapshot done on node id %d", s.shardID, s.dragon.cnf.NodeID)
	return nil
}

func (s *ShardOnDiskStateMachine) RecoverFromSnapshot(reader io.Reader, i <-chan struct{}) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	log.Debugf("data shard %d recover from snapshot on node %d", s.shardID, s.dragon.cnf.NodeID)
	s.dedupSequences = make(map[string]uint64)
	startPrefix := common.AppendUint64ToBufferBE(make([]byte, 0, 8), s.shardID)
	endPrefix := common.AppendUint64ToBufferBE(make([]byte, 0, 8), s.shardID+1)
	log.Debugf("Restoring data snapshot on node %d shardid %d", s.dragon.cnf.NodeID, s.shardID)
	err := restoreSnapshotDataFromReader(s.dragon.pebble, startPrefix, endPrefix, reader, s.dragon.ingestDir)
	if err != nil {
		return errors.WithStack(err)
	}
	if err := s.loadDedupCache(); err != nil {
		return err
	}
	_, receiverSequence, err := s.loadSequences(s.dragon.pebble, s.shardID)
	if err != nil {
		return err
	}
	s.receiverSequence = receiverSequence
	leaderNodeID, term, err := s.loadLeaderInfo(s.dragon.pebble, s.shardID)
	if err != nil {
		return err
	}
	s.leaderNodeID = leaderNodeID
	s.leaderTerm = term
	log.Debugf("data shard %d recover from snapshot done on node %d", s.shardID, s.dragon.cnf.NodeID)
	atomic.AddInt64(&s.dragon.restoreSnapshotCount, 1)
	return nil
}

func (s *ShardOnDiskStateMachine) Close() error {
	s.dragon.unregisterShardSM(s.shardID)
	return nil
}

func (s *ShardOnDiskStateMachine) loadDedupCache() error {
	// Load duplicate cache
	s.dedupSequences = make(map[string]uint64)
	startPrefix := table.EncodeTableKeyPrefix(common.ForwardDedupTableID, s.shardID, 16)
	endPrefix := table.EncodeTableKeyPrefix(common.ForwardDedupTableID+1, s.shardID, 16)
	pairs, err := s.dragon.LocalScan(startPrefix, endPrefix, math.MaxInt)
	if err != nil {
		return err
	}
	for _, kvPair := range pairs {
		oid := kvPair.Key[16:32]
		sequence, _ := common.ReadUint64FromBufferBE(kvPair.Value, 0)
		s.dedupSequences[string(oid)] = sequence
	}
	return nil
}

func (s *ShardOnDiskStateMachine) tableDropped(tableID uint64) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.droppedTables.Add(tableID, struct{}{})
}
