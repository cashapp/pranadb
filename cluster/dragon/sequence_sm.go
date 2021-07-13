package dragon

import (
	"github.com/cockroachdb/pebble"
	"github.com/lni/dragonboat/v3/statemachine"
	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/table"
	"io"
)

const (
	seqStateMachineUpdatedOK uint64 = 1
)

func (d *Dragon) newSequenceStateMachine(clusterID uint64, nodeID uint64) statemachine.IStateMachine {
	return &sequenceStateMachine{
		dragon:  d,
		shardID: clusterID,
	}
}

type sequenceStateMachine struct {
	dragon  *Dragon
	shardID uint64
}

func (s *sequenceStateMachine) Update(buff []byte) (statemachine.Result, error) {
	seqName, _ := common.DecodeString(buff, 0)
	keyBuff := make([]byte, 0, 32)
	keyBuff = common.AppendUint64ToBufferLittleEndian(keyBuff, table.SequenceTableID)
	keyBuff = common.AppendUint64ToBufferLittleEndian(keyBuff, s.shardID)
	keyBuff = common.EncodeString(seqName, keyBuff)
	v, err := localGet(s.dragon.pebble, keyBuff)
	if err != nil {
		return statemachine.Result{}, err
	}
	var seqVal uint64
	var seqBuff []byte
	if v != nil {
		seqVal = common.ReadUint64FromBufferLittleEndian(v, 0)
		seqBuff = v
	} else {
		seqVal = cluster.UserTableIDBase
		seqBuff = make([]byte, 8)
	}
	vBuff := make([]byte, 0, 8)
	vBuff = common.AppendUint64ToBufferLittleEndian(vBuff, seqVal+1)
	err = s.dragon.pebble.Set(keyBuff, vBuff, &pebble.WriteOptions{Sync: false})
	if err != nil {
		return statemachine.Result{}, err
	}
	return statemachine.Result{Value: seqStateMachineUpdatedOK, Data: seqBuff}, nil
}

func (s *sequenceStateMachine) Lookup(i interface{}) (interface{}, error) {
	panic("should not be called")
}

func (s *sequenceStateMachine) SaveSnapshot(writer io.Writer, collection statemachine.ISnapshotFileCollection, i <-chan struct{}) error {
	// TODO
	return nil
}

func (s *sequenceStateMachine) RecoverFromSnapshot(reader io.Reader, files []statemachine.SnapshotFile, i <-chan struct{}) error {
	// TODO
	return nil
}

func (s *sequenceStateMachine) Close() error {
	return nil
}
