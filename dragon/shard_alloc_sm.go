package dragon

import (
	"github.com/cockroachdb/pebble"
	"github.com/lni/dragonboat/v3/statemachine"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/table"
	"io"
	"io/ioutil"
)

const (
	shardAllocationUpdateSucceeded uint64 = 1
	shardAllocationAlreadySet      uint64 = 2
	shardAllocKeyName              string = "shard_alloc"
)

func (d *Dragon) newShardAllocationStateMachine(clusterID uint64, nodeID uint64) statemachine.IStateMachine {
	return &shardAllocationStateMachine{
		shardID:         clusterID,
		pebble:          d.pebble,
		shardAllocation: &shardAllocation{},
	}
}

type shardAllocationStateMachine struct {
	shardID         uint64
	pebble          *pebble.DB
	shardAllocation *shardAllocation
}

func (s *shardAllocationStateMachine) maybeLoadShardAllocation() error {
	if len(s.shardAllocation.allocations) != 0 {
		return nil
	}
	vBytes, err := localGet(s.pebble, s.shardAllocationKey())
	if err != nil {
		return err
	}
	if vBytes != nil {
		s.shardAllocation.deserialize(vBytes)
	}
	return nil
}

func (s *shardAllocationStateMachine) shardAllocationKey() []byte {
	keyBuff := make([]byte, 0, 32)
	keyBuff = common.AppendUint64ToBufferLittleEndian(keyBuff, table.ShardAllocTableID)
	keyBuff = common.AppendUint64ToBufferLittleEndian(keyBuff, s.shardID)
	keyBuff = common.EncodeString(shardAllocKeyName, keyBuff)
	return keyBuff
}

func (s *shardAllocationStateMachine) Update(bytes []byte) (statemachine.Result, error) {
	err := s.maybeLoadShardAllocation()
	if err != nil {
		return statemachine.Result{}, err
	}
	// Cluster has fixed number of shards (for now), so can only set this once
	if len(s.shardAllocation.allocations) != 0 {
		return statemachine.Result{Value: shardAllocationAlreadySet}, nil
	}
	err = s.pebble.Set(s.shardAllocationKey(), bytes, &pebble.WriteOptions{Sync: false})
	if err != nil {
		return statemachine.Result{}, err
	}
	s.shardAllocation.deserialize(bytes)
	return statemachine.Result{Value: shardAllocationUpdateSucceeded}, nil
}

func (s *shardAllocationStateMachine) Lookup(i interface{}) (interface{}, error) {
	err := s.maybeLoadShardAllocation()
	if err != nil {
		return nil, err
	}
	var buff []byte
	return s.shardAllocation.serialize(buff), nil
}

func (s *shardAllocationStateMachine) SaveSnapshot(writer io.Writer, collection statemachine.ISnapshotFileCollection, i <-chan struct{}) error {
	err := s.maybeLoadShardAllocation()
	if err != nil {
		return err
	}
	var buff []byte
	buff = s.shardAllocation.serialize(buff)
	_, err = writer.Write(buff)
	return err
}

func (s *shardAllocationStateMachine) RecoverFromSnapshot(reader io.Reader, files []statemachine.SnapshotFile, i <-chan struct{}) error {
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		return err
	}
	s.shardAllocation.deserialize(data)
	// TODO update in storage?
	return nil
}

func (s *shardAllocationStateMachine) Close() error {
	return nil
}
