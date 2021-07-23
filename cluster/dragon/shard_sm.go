package dragon

import (
	"fmt"
	"github.com/cockroachdb/pebble"
	"github.com/lni/dragonboat/v3/statemachine"
	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/common"
	"io"
	"log"
)

const (
	shardStateMachineCommandWrite        byte = 1
	shardStateMachineCommandForwardWrite      = 2
	shardStateMachineCommandRemoveNode        = 3
	shardStateMachineCommandDeletePrefix      = 4

	shardStateMachineResponseOK uint64 = 1
)

func newShardStateMachine(d *Dragon, shardID uint64, nodeID int, nodeIDs []int) statemachine.IStateMachine {
	processor := calcProcessingNode(nodeIDs, shardID, nodeID)
	ssm := shardStateMachine{
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

// TODO implement IOnDiskStateMachine
type shardStateMachine struct {
	nodeID        int
	shardID       uint64
	dragon        *Dragon
	nodeIDs       []int
	processor     bool
	shardListener cluster.ShardListener
}

func (s *shardStateMachine) Update(bytes []byte) (statemachine.Result, error) {
	command := bytes[0]
	switch command {
	case shardStateMachineCommandWrite:
		return s.handleWrite(bytes, false)
	case shardStateMachineCommandForwardWrite:
		return s.handleWrite(bytes, true)
	case shardStateMachineCommandRemoveNode:
		return s.handleRemoveNode(bytes)
	case shardStateMachineCommandDeletePrefix:
		return s.handleDeletePrefix(bytes)
	default:
		panic(fmt.Sprintf("unexpected command %d", command))
	}
}

func (s *shardStateMachine) handleWrite(bytes []byte, forward bool) (statemachine.Result, error) {
	puts, deletes := deserializeWriteBatch(bytes, 1)
	err := s.writeBatchLocal(puts, deletes)
	if err != nil {
		return statemachine.Result{}, err
	}
	// A forward write is a write which forwards a batch of rows from one shard to another
	// In this case we want to trigger processing of those rows, if we're the processor
	if forward && s.processor {
		s.shardListener.RemoteWriteOccurred()
	}
	return statemachine.Result{
		Value: shardStateMachineResponseOK,
	}, nil
}

func (s *shardStateMachine) handleRemoveNode(bytes []byte) (statemachine.Result, error) {
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
		return statemachine.Result{
			Value: shardStateMachineResponseOK,
		}, nil
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
	return statemachine.Result{
		Value: shardStateMachineResponseOK,
	}, nil
}

func (s *shardStateMachine) handleDeletePrefix(bytes []byte) (statemachine.Result, error) {
	offset := 1
	lenPrefix := int(common.ReadUint32FromBufferLittleEndian(bytes, offset))
	offset += 4
	prefix := bytes[offset : offset+lenPrefix]
	log.Printf("Shard sm on node %d and shard %d, deleting all data for prefix %v", s.dragon.nodeID, s.shardID, prefix)
	endRange := copyByteSlice(prefix)
	// We just increment the key - it just needs to be anything larger than the prefix but of the same length
	for i, b := range endRange {
		if b < 255 {
			endRange[i] = b + 1
			break
		}
		if i == len(endRange)-1 {
			panic("cannot increment key - all bits set")
		}
	}
	err := s.dragon.pebble.DeleteRange(prefix, endRange, &pebble.WriteOptions{})
	if err != nil {
		return statemachine.Result{}, err
	}
	return statemachine.Result{
		Value: shardStateMachineResponseOK,
	}, nil
}

func (s *shardStateMachine) writeBatchLocal(puts []cluster.KVPair, deletes [][]byte) error {
	batch := s.dragon.pebble.NewBatch()
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
	return s.dragon.pebble.Apply(batch, &pebble.WriteOptions{Sync: false})
}

func (s *shardStateMachine) checkKey(key []byte) {
	if s.dragon.testDragon {
		return
	}
	// Sanity check
	sid := common.ReadUint64FromBufferLittleEndian(key, 8)
	if s.shardID != sid {
		panic(fmt.Sprintf("invalid key in sm write, expected %d actual %d", s.shardID, sid))
	}
}

func (s *shardStateMachine) Lookup(i interface{}) (interface{}, error) {
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

func (s *shardStateMachine) SaveSnapshot(writer io.Writer, collection statemachine.ISnapshotFileCollection, i <-chan struct{}) error {
	_, err := writer.Write([]byte{0})
	return err
}

func (s *shardStateMachine) RecoverFromSnapshot(reader io.Reader, files []statemachine.SnapshotFile, i <-chan struct{}) error {
	_, err := reader.Read([]byte{0})
	return err
}

func (s *shardStateMachine) Close() error {
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
