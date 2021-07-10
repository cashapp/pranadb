package dragon

import (
	"errors"
	"fmt"
	"github.com/cockroachdb/pebble"
	"github.com/lni/dragonboat/v3/statemachine"
	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/common"
	"io"
)

const (
	shardStateMachineCommandWrite        byte = 1
	shardStateMachineCommandForwardWrite      = 2
	shardStateMachineCommandRemoveNode        = 3

	shardStateMachineResponseOK uint64 = 1
)

func newShardStateMachine(d *Dragon, shardID uint64, nodeID int, nodeIDs []int) statemachine.IStateMachine {
	leader := calcLeader(nodeIDs, shardID, nodeID)
	ssm := shardStateMachine{
		nodeID:  nodeID,
		nodeIDs: nodeIDs,
		shardID: shardID,
		dragon:  d,
		leader:  leader,
	}
	if leader {
		if d.shardListenerFactory == nil {
			panic("no shard listener")
		}
		ssm.shardListener = d.shardListenerFactory.CreateShardListener(shardID)
	}
	return &ssm
}

type shardStateMachine struct {
	nodeID        int
	shardID       uint64
	dragon        *Dragon
	nodeIDs       []int
	leader        bool
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
	default:
		panic(fmt.Sprintf("unexpected command %d", command))
	}
}

func (s *shardStateMachine) handleWrite(bytes []byte, forward bool) (statemachine.Result, error) {
	puts, deletes := deserializeWriteBatch(bytes, 1)
	err := writeBatchLocal(s.dragon.pebble, puts, deletes)
	if err != nil {
		return statemachine.Result{}, err
	}
	// A forward write is a write which forwards a batch of rows from one shard to another
	// In this case we want to trigger processing of those rows, if we're the leader
	if forward && s.leader {
		s.shardListener.RemoteWriteOccurred()
	}
	return statemachine.Result{
		Value: shardStateMachineResponseOK,
	}, nil
}

func (s *shardStateMachine) handleRemoveNode(bytes []byte) (statemachine.Result, error) {
	n := int(common.ReadUint32FromBufferLittleEndian(bytes, 1))
	var newNodes []int
	for _, nid := range s.nodeIDs {
		if n != nid {
			newNodes = append(newNodes, nid)
		}
	}
	if len(newNodes) == len(s.nodeIDs) {
		return statemachine.Result{}, errors.New("cannot find node to remove")
	}
	s.nodeIDs = newNodes
	s.leader = calcLeader(s.nodeIDs, s.shardID, s.nodeID)
	if s.leader {
		// We're a leader and weren't before
		s.shardListener = s.dragon.shardListenerFactory.CreateShardListener(s.shardID)
	} else {
		// We're not a leader any more, close the listener
		s.shardListener.Close()
	}
	return statemachine.Result{}, nil
}

func writeBatchLocal(peb *pebble.DB, puts []cluster.KVPair, deletes [][]byte) error {
	batch := peb.NewBatch()
	for _, kvPair := range puts {
		err := batch.Set(kvPair.Key, kvPair.Value, nil)
		if err != nil {
			return err
		}
	}
	for _, k := range deletes {
		err := batch.Delete(k, nil)
		if err != nil {
			return err
		}
	}
	return peb.Apply(batch, &pebble.WriteOptions{Sync: false})
}

func (s *shardStateMachine) Lookup(i interface{}) (interface{}, error) {
	panic("should not be called")
}

func (s *shardStateMachine) SaveSnapshot(writer io.Writer, collection statemachine.ISnapshotFileCollection, i <-chan struct{}) error {
	// TODO
	return nil
}

func (s *shardStateMachine) RecoverFromSnapshot(reader io.Reader, files []statemachine.SnapshotFile, i <-chan struct{}) error {
	// TODO
	return nil
}

func (s *shardStateMachine) Close() error {
	return nil
}

func calcLeader(nodeIDs []int, shardID uint64, nodeID int) bool {
	leaderNode := nodeIDs[shardID%uint64(len(nodeIDs))]
	return nodeID == leaderNode
}
