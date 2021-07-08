package dragon

import (
	"github.com/cockroachdb/pebble"
	"github.com/lni/dragonboat/v3/statemachine"
	"github.com/squareup/pranadb/cluster"
	"io"
)

const (
	shardStateMachineUpdatedOK uint64 = 1
)

func (d *Dragon) newShardStateMachine(clusterID uint64, nodeID uint64) statemachine.IStateMachine {
	return &shardStateMachine{
		shardID: clusterID,
		dragon:  d,
	}
}

type shardStateMachine struct {
	shardID uint64
	dragon  *Dragon
}

func (s *shardStateMachine) Update(bytes []byte) (statemachine.Result, error) {
	puts, deletes := deserializeWriteBatch(bytes, 0)
	err := writeBatchLocal(s.dragon.pebble, puts, deletes)
	if err != nil {
		return statemachine.Result{}, err
	}
	// TODO we can avoid this by pushing whether a node is leader into the state machine - this
	// don't change that often compared to writes
	isLeader := s.dragon.isLeader(s.shardID)
	if isLeader {
		s.dragon.callRemoteWriteHandler(s.shardID)
	}
	return statemachine.Result{
		Value: shardStateMachineUpdatedOK,
	}, nil
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
