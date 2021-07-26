package dragon

import (
	"fmt"
	"github.com/cockroachdb/pebble"
	"github.com/lni/dragonboat/v3/statemachine"
	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/table"
	"io"
	"log"
)

const (
	shardStateMachineCommandWrite             byte = 1
	shardStateMachineCommandForwardWrite           = 2
	shardStateMachineCommandRemoveNode             = 3
	shardStateMachineCommandDeleteRangePrefix      = 4

	shardStateMachineResponseOK uint64 = 1

	snapshotSaveBufferSize      = 8 * 1024
	snapshotRecoverBufferSize   = 8 * 1024
	maxSnapshotRecoverBatchSize = 10000
)

func newShardODStateMachine(d *Dragon, shardID uint64, nodeID int, nodeIDs []int) statemachine.IOnDiskStateMachine {
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
	nodeID        int
	shardID       uint64
	dragon        *Dragon
	nodeIDs       []int
	processor     bool
	shardListener cluster.ShardListener
}

func (s *ShardOnDiskStateMachine) Open(stopc <-chan struct{}) (uint64, error) {
	return loadLastProcessedRaftIndex(s.dragon.pebble, s.shardID)
}

func (s *ShardOnDiskStateMachine) Update(entries []statemachine.Entry) ([]statemachine.Entry, error) {
	hasForward := false
	hasWrites := false
	batch := s.dragon.pebble.NewBatch()
	for i, entry := range entries {
		cmdBytes := entry.Cmd
		command := cmdBytes[0]
		switch command {
		case shardStateMachineCommandWrite, shardStateMachineCommandForwardWrite:
			if err := s.handleWrite(batch, cmdBytes); err != nil {
				return nil, err
			}
			hasWrites = true
			if command == shardStateMachineCommandForwardWrite {
				hasForward = true
			}
		case shardStateMachineCommandRemoveNode:
			s.handleRemoveNode(cmdBytes)
		case shardStateMachineCommandDeleteRangePrefix:
			err := s.handleDeleteRange(batch, cmdBytes)
			if err != nil {
				return nil, err
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
	common.AppendUint64ToBufferLittleEndian(vb, lastLogIndex)
	if err := batch.Set(key, vb, nil); err != nil {
		return nil, err
	}
	if err := writeLastIndexValue(batch, lastLogIndex, s.shardID); err != nil {
		return nil, err
	}

	if err := s.dragon.pebble.Apply(batch, nosyncWriteOptions); err != nil {
		return nil, err
	}

	// A forward write is a write which forwards a batch of rows from one shard to another
	// In this case we want to trigger processing of those rows, if we're the processor
	if hasWrites && hasForward && s.processor {
		s.shardListener.RemoteWriteOccurred()
	}
	return entries, nil
}

func (s *ShardOnDiskStateMachine) handleWrite(batch *pebble.Batch, bytes []byte) error {
	puts, deletes := deserializeWriteBatch(bytes, 1)
	for _, kvPair := range puts {
		s.checkKey(kvPair.Key)
		log.Printf("Writing into pebble on node %d k:%v v:%v", s.nodeID, kvPair.Key, kvPair.Value)
		err := batch.Set(kvPair.Key, kvPair.Value, nil)
		if err != nil {
			return err
		}
	}
	for _, k := range deletes {
		s.checkKey(k)
		log.Printf("Deleting from pebble on node %d k:%v", s.nodeID, k)
		err := batch.Delete(k, nil)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *ShardOnDiskStateMachine) handleRemoveNode(bytes []byte) {
	n := int(common.ReadUint32FromBufferLittleEndian(bytes, 1))
	found := false
	for _, nid := range s.nodeIDs {
		if n == nid {
			found = true
			break
		}
	}
	if !found {
		// This is OK - when a membership change occurs, every node in the cluster will get the notification about the change
		// and the state machine will be updated from every node, so it may already have been updated
		return
	}
	var newNodes []int
	for _, nid := range s.nodeIDs {
		if n != nid {
			newNodes = append(newNodes, nid)
		}
	}
	s.nodeIDs = newNodes
	newProcessor := calcProcessingNode(s.nodeIDs, s.shardID, s.nodeID)
	if newProcessor != s.processor {
		s.processor = newProcessor
		if s.shardListener != nil {
			s.shardListener.Close()
		}
		if s.processor {
			// We're the processor
			s.shardListener = s.dragon.shardListenerFactory.CreateShardListener(s.shardID)
		}
	}
}

func (s *ShardOnDiskStateMachine) handleDeleteRange(batch *pebble.Batch, bytes []byte) error {
	offset := 1
	lenStartPrefix := int(common.ReadUint32FromBufferLittleEndian(bytes, offset))
	offset += 4
	startPrefix := bytes[offset : offset+lenStartPrefix]
	offset += lenStartPrefix

	lenEndPrefix := int(common.ReadUint32FromBufferLittleEndian(bytes, offset))
	offset += 4
	endPrefix := bytes[offset : offset+lenEndPrefix]

	log.Printf("Shard sm on node %d and shard %d, deleting all data between %v and %v", s.dragon.nodeID, s.shardID, startPrefix, endPrefix)

	return batch.DeleteRange(startPrefix, endPrefix, &pebble.WriteOptions{})
}

func (s *ShardOnDiskStateMachine) checkKey(key []byte) {
	if s.dragon.testDragon {
		return
	}
	// Sanity check
	sid := common.ReadUint64FromBufferBigEndian(key, 0)
	if s.shardID != sid {
		panic(fmt.Sprintf("invalid key in sm write, expected %d actual %d", s.shardID, sid))
	}
}

func (s *ShardOnDiskStateMachine) Lookup(i interface{}) (interface{}, error) {
	buff, ok := i.([]byte)
	if !ok {
		panic("expected []byte")
	}
	queryInfo := &cluster.QueryExecutionInfo{}
	err := queryInfo.Deserialize(buff)
	if err != nil {
		return nil, err
	}
	rows, err := s.dragon.remoteQueryExecutionCallback.ExecuteRemotePullQuery(queryInfo)
	if err != nil {
		return nil, err
	}
	buff = rows.Serialize()
	return buff, nil
}

func (s *ShardOnDiskStateMachine) Sync() error {
	return syncPebble(s.dragon.pebble)
}

func (s *ShardOnDiskStateMachine) PrepareSnapshot() (interface{}, error) {
	snapshot := s.dragon.pebble.NewSnapshot()
	return snapshot, nil
}

func (s *ShardOnDiskStateMachine) SaveSnapshot(i interface{}, writer io.Writer, _ <-chan struct{}) error {
	snapshot, ok := i.(*pebble.Snapshot)
	if !ok {
		panic("not a snapshot")
	}
	prefix := make([]byte, 0, 8)
	prefix = common.AppendUint64ToBufferLittleEndian(prefix, s.shardID)
	return saveSnapshotDataToWriter(snapshot, prefix, writer, s.shardID)
}

func (s *ShardOnDiskStateMachine) RecoverFromSnapshot(reader io.Reader, i <-chan struct{}) error {
	startPrefix := common.AppendUint64ToBufferLittleEndian(make([]byte, 0, 8), s.shardID)
	endPrefix := common.AppendUint64ToBufferLittleEndian(make([]byte, 0, 8), s.shardID+1)
	return restoreSnapshotDataFromReader(s.dragon.pebble, startPrefix, endPrefix, reader)
}

func (s *ShardOnDiskStateMachine) Close() error {
	// Nothing much to do here
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
