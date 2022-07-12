package dragon

import (
	"fmt"
	"github.com/cockroachdb/pebble"
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
	"time"
)

const (
	shardStateMachineLookupPing          byte   = 1
	shardStateMachineLookupQuery         byte   = 2
	shardStateMachineCommandWrite        byte   = 1
	shardStateMachineCommandForwardWrite byte   = 2
	shardStateMachineResponseOK          uint64 = 1
)

func newShardODStateMachine(d *Dragon, shardID uint64, nodeID int, nodeIDs []int) *ShardOnDiskStateMachine {
	processor := calcProcessingNode(nodeIDs, shardID, nodeID)
	ssm := ShardOnDiskStateMachine{
		nodeID:    nodeID,
		nodeIDs:   nodeIDs,
		shardID:   shardID,
		dragon:    d,
		processor: processor,
	}
	if processor {
		if d.shardListenerFactory == nil {
			panic("no shard listener")
		}
		ssm.shardListener = d.shardListenerFactory.CreateShardListener(shardID)
	}
	return &ssm
}

type ShardOnDiskStateMachine struct {
	nodeID           int
	shardID          uint64
	dragon           *Dragon
	nodeIDs          []int
	processor        bool
	shardListener    cluster.ShardListener
	dedupSequences   map[string]uint64 // TODO use byteslicemap or similar
	receiverSequence uint64
	forwardRows      []cluster.ForwardRow
	lock             sync.Mutex
}

func (s *ShardOnDiskStateMachine) Open(stopc <-chan struct{}) (uint64, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.dragon.registerShardSM(s.shardID)
	if err := s.loadDedupCache(); err != nil {
		return 0, err
	}
	lastRaftIndex, receiverSequence, err := s.loadSequences(s.dragon.pebble, s.shardID)
	if err != nil {
		return 0, err
	}
	s.receiverSequence = receiverSequence
	return lastRaftIndex, nil
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

func (s *ShardOnDiskStateMachine) Update(entries []statemachine.Entry) ([]statemachine.Entry, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	batch := s.dragon.pebble.NewBatch()
	timestamp := int64(time.Now().Sub(common.UnixStart))
	for i, entry := range entries {
		cmdBytes := entry.Cmd
		command := cmdBytes[0]
		switch command {
		case shardStateMachineCommandForwardWrite:
			if s.forwardRows == nil && s.processor {
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
	if len(s.forwardRows) > 0 {
		s.shardListener.RemoteWriteOccurred(s.forwardRows)
		s.forwardRows = nil
	}
	return entries, nil
}

func (s *ShardOnDiskStateMachine) handleWrite(batch *pebble.Batch, bytes []byte, forward bool, timestamp int64) error {
	puts, deletes := s.deserializeWriteBatch(bytes, 1, forward)
	for _, kvPair := range puts {

		var key []byte
		if forward {
			//enableDupDetection := kvPair.Key[0] == 1
			//dedupKey := kvPair.Key[1:25]           // Next 24 bytes is the dedup key

			//if enableDupDetection {
			//	ignore, err := s.checkDedup(dedupKey, batch)
			//	if err != nil {
			//		return err
			//	}
			//	if ignore {
			//		continue
			//	}
			//}

			remoteConsumerBytes := kvPair.Key[25:] // The rest is just the remote consumer id

			// For a write into the receiver table (forward write) the key is constructed as follows:
			// shard_id|receiver_table_id|receiver_sequence|remote_consumer_id
			key = table.EncodeTableKeyPrefix(common.ReceiverTableID, s.shardID, 40)
			key = common.AppendUint64ToBufferBE(key, s.receiverSequence)
			key = append(key, remoteConsumerBytes...)
			s.receiverSequence++

			if s.processor {
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
			s.checkKey(key)
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
	return nil
}

// We deserialize into simple slices for puts and deletes as we don't need the actual WriteBatch instance in the
// state machine
func (s *ShardOnDiskStateMachine) deserializeWriteBatch(buff []byte, offset int, forward bool) (puts []cluster.KVPair, deletes [][]byte) {
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

func (s *ShardOnDiskStateMachine) checkKey(key []byte) {
	if s.dragon.cnf.TestServer {
		return
	}
	// Sanity check
	sid, _ := common.ReadUint64FromBufferBE(key, 0)
	if s.shardID != sid {
		panic(fmt.Sprintf("invalid key in sm write, expected %d actual %d", s.shardID, sid))
	}
}

func (s *ShardOnDiskStateMachine) Lookup(i interface{}) (interface{}, error) {
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
			var buff []byte
			buff = append(buff, 0) // Zero byte signifies error
			buff = append(buff, err.Error()...)
			// Note - we don't send back an error to Dragon if a query failed - we only return an error
			// for an unrecoverable error.
			return buff, nil
		}
		b := rows.Serialize()
		buff := make([]byte, 0, 1+len(b))
		buff = append(buff, 1) // 1 signifies no error
		buff = append(buff, b...)
		return buff, nil
	} else {
		panic("invalid lookup type")
	}
}

func (s *ShardOnDiskStateMachine) Sync() error {
	return syncPebble(s.dragon.pebble)
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
	log.Debugf("data shard %d recover from snapshot done on node %d", s.shardID, s.dragon.cnf.NodeID)
	atomic.AddInt64(&s.dragon.restoreSnapshotCount, 1)
	return nil
}

func (s *ShardOnDiskStateMachine) Close() error {
	s.dragon.unregisterShardSM(s.shardID)
	return nil
}

// One of the replicas is chosen in a deterministic way to do the processing for the shard - i.e. to handle any
// incoming rows. It doesn't matter whether this replica is the raft leader or not, but every raft replica needs
// to come to the same decision as to who is the processor - that is why we handle the remove node event through
// the same state machine as processing writes.
func calcProcessingNode(nodeIDs []int, shardID uint64, nodeID int) bool {
	leaderNode := nodeIDs[shardID%uint64(len(nodeIDs))]
	return nodeID == leaderNode
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
