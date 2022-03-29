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
)

const (
	shardStateMachineLookupPing                   byte   = 1
	shardStateMachineLookupQuery                  byte   = 2
	shardStateMachineCommandWrite                 byte   = 1
	shardStateMachineCommandForwardWrite          byte   = 2
	shardStateMachineCommandForwardWriteWithDedup byte   = 3
	shardStateMachineCommandDeleteRangePrefix     byte   = 4
	shardStateMachineResponseOK                   uint64 = 1
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
	nodeID         int
	shardID        uint64
	dragon         *Dragon
	nodeIDs        []int
	processor      bool
	shardListener  cluster.ShardListener
	dedupSequences map[string]uint64 // TODO use byteslicemap or similar
	lock           sync.Mutex
}

func (s *ShardOnDiskStateMachine) Open(stopc <-chan struct{}) (uint64, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	log.Debugf("Opening shard state machine with shard id %d", s.shardID)
	s.dragon.registerShardSM(s.shardID)
	if err := s.loadDedupCache(); err != nil {
		return 0, err
	}
	return loadLastProcessedRaftIndex(s.dragon.pebble, s.shardID)
}

func (s *ShardOnDiskStateMachine) Update(entries []statemachine.Entry) ([]statemachine.Entry, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	hasForward := false //nolint:ifshort
	hasIngest := false  //nolint:ifshort
	batch := s.dragon.pebble.NewBatch()
	for i, entry := range entries {
		cmdBytes := entry.Cmd
		command := cmdBytes[0]
		switch command {
		case shardStateMachineCommandForwardWriteWithDedup:
			if err := s.handleWrite(batch, cmdBytes, true); err != nil {
				return nil, errors.WithStack(err)
			}
			hasIngest = true
		case shardStateMachineCommandForwardWrite:
			if err := s.handleWrite(batch, cmdBytes, false); err != nil {
				return nil, errors.WithStack(err)
			}
			hasForward = true
		case shardStateMachineCommandWrite:
			if err := s.handleWrite(batch, cmdBytes, false); err != nil {
				return nil, errors.WithStack(err)
			}
		case shardStateMachineCommandDeleteRangePrefix:
			err := s.handleDeleteRange(batch, cmdBytes)
			if err != nil {
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
	if err := writeLastIndexValue(batch, lastLogIndex, s.shardID); err != nil {
		return nil, errors.WithStack(err)
	}

	if err := s.dragon.pebble.Apply(batch, nosyncWriteOptions); err != nil {
		return nil, errors.WithStack(err)
	}

	// A forward write is a write which forwards a batch of rows from one shard to another
	// In this case we want to trigger processing of those rows, if we're the processor
	if hasForward {
		s.maybeTriggerRemoteWriteOccurred(false)
	} else if hasIngest {
		s.maybeTriggerRemoteWriteOccurred(true)
	}
	return entries, nil
}

func (s *ShardOnDiskStateMachine) maybeTriggerRemoteWriteOccurred(ingest bool) {
	// A forward write is a write which forwards a batch of rows from one shard to another
	// In this case we want to trigger processing of those rows, if we're the processor
	if s.processor {
		s.shardListener.RemoteWriteOccurred(ingest)
	}
}

func (s *ShardOnDiskStateMachine) handleWrite(batch *pebble.Batch, bytes []byte, dedup bool) error {
	puts, deletes := deserializeWriteBatch(bytes, 1)
	for _, kvPair := range puts {
		if dedup {
			ok, err := s.checkDedup(kvPair.Key, batch)
			if err != nil {
				return err
			}
			if !ok {
				continue
			}
		}
		s.checkKey(kvPair.Key)
		err := batch.Set(kvPair.Key, kvPair.Value, nil)
		if err != nil {
			return errors.WithStack(err)
		}
	}
	for _, k := range deletes {
		if dedup {
			ok, err := s.checkDedup(k, batch)
			if err != nil {
				return err
			}
			if !ok {
				continue
			}
		}
		s.checkKey(k)
		err := batch.Delete(k, nil)
		if err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func (s *ShardOnDiskStateMachine) getDedupSequence(originatorID []byte) (uint64, bool) {
	seq, ok := s.dedupSequences[string(originatorID)]
	return seq, ok
}

func (s *ShardOnDiskStateMachine) checkDedup(key []byte, batch *pebble.Batch) (bool, error) {
	// Duplicate detection
	// Duplicate updates can occur when the same rows are ingested from Kafka after failure and recovery but
	// they can also in normal use without Prana failure when a call to syncPropose times out and is retried.
	// When it times out the changes might have been applied to the SM and when retried the same data will be
	// resent. For an IOnDiskStatemachine we MUST use a no-op client to make the propose so it is up to us to
	// make sure the SM is idempotent. The duplicate detection provides this guarantee

	// First 16 bytes is [shard_id, table_id]
	// Dedup key is next 24 bytes and structured as originator_id (16 bytes) then sequence (8 bytes)
	oid := key[16 : 16+16]
	soid := string(oid)
	seq, _ := common.ReadUint64FromBufferBE(key, 16+16)
	prevSeq, ok := s.getDedupSequence(oid)
	if ok && prevSeq >= seq {
		log.Debugf("Duplicate forward row detected in shard %d - soid %s sequence %d prevSequence %d ignoring it", s.shardID, soid, seq, prevSeq)
		return false, nil
	}
	// Persist the duplicate entry
	dupID := table.EncodeTableKeyPrefix(common.ForwardDedupTableID, s.shardID, 16+24)
	dupID = append(dupID, oid...)
	if err := batch.Set(dupID, key[32:32+8], nil); err != nil {
		return false, errors.WithStack(err)
	}
	s.dedupSequences[soid] = seq
	return true, nil
}

func (s *ShardOnDiskStateMachine) handleDeleteRange(batch *pebble.Batch, bytes []byte) error {
	offset := 1
	lsp, offset := common.ReadUint32FromBufferLE(bytes, offset)
	lenStartPrefix := int(lsp)
	startPrefix := bytes[offset : offset+lenStartPrefix]
	offset += lenStartPrefix

	lenEndPrefix, offset := common.ReadUint32FromBufferLE(bytes, offset)
	endPrefix := bytes[offset : offset+int(lenEndPrefix)]

	return batch.DeleteRange(startPrefix, endPrefix, nosyncWriteOptions)
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
	s.lock.Lock()
	defer s.lock.Unlock()
	log.Debugf("data shard %d saving snapshot", s.shardID)
	snapshot, ok := i.(*pebble.Snapshot)
	if !ok {
		panic("not a snapshot")
	}
	prefix := make([]byte, 0, 8)
	prefix = common.AppendUint64ToBufferBE(prefix, s.shardID)
	log.Printf("Saving data snapshot on node id %d for shard id %d prefix is %v", s.dragon.cnf.NodeID, s.shardID, prefix)
	err := saveSnapshotDataToWriter(snapshot, prefix, writer, s.shardID)
	log.Debugf("data shard %d save snapshot done", s.shardID)
	return err
}

func (s *ShardOnDiskStateMachine) RecoverFromSnapshot(reader io.Reader, i <-chan struct{}) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	log.Debugf("data shard %d recover from snapshot", s.shardID)
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
	s.maybeTriggerRemoteWriteOccurred(true)
	s.maybeTriggerRemoteWriteOccurred(false)
	log.Debugf("data shard %d recover from snapshot done", s.shardID)
	return nil
}

func (s *ShardOnDiskStateMachine) Close() error {
	log.Debugf("Closing shard state machine with shard id %d", s.shardID)
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
		seq, _ := common.ReadUint64FromBufferBE(kvPair.Value, 0)
		s.dedupSequences[string(oid)] = seq
	}
	return nil
}
