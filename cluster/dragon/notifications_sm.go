package dragon

import (
	"github.com/lni/dragonboat/v3/statemachine"
	"github.com/pkg/errors"
	"io"

	"github.com/squareup/pranadb/cluster"
)

const (
	notificationsStateMachineUpdatedOK uint64 = 1
)

func (d *Dragon) newNotificationsStateMachine(shardID uint64, _ uint64) statemachine.IStateMachine {
	return &notificationsStateMachine{
		dragon:  d,
		shardID: shardID,
	}
}

// TODO implement IOnDiskStateMachine
type notificationsStateMachine struct {
	shardID uint64
	dragon  *Dragon
}

func (s *notificationsStateMachine) Update(buff []byte) (statemachine.Result, error) {
	notification, err := cluster.DeserializeNotification(buff)
	if err != nil {
		return statemachine.Result{}, errors.WithStack(err)
	}
	s.dragon.handleNotification(notification)
	return statemachine.Result{Value: notificationsStateMachineUpdatedOK, Data: nil}, nil
}

func (s *notificationsStateMachine) Lookup(i interface{}) (interface{}, error) {
	panic("should not be called")
}

func (s *notificationsStateMachine) SaveSnapshot(writer io.Writer, collection statemachine.ISnapshotFileCollection, i <-chan struct{}) error {
	_, err := writer.Write([]byte{0})
	return err
}

func (s *notificationsStateMachine) RecoverFromSnapshot(reader io.Reader, files []statemachine.SnapshotFile, i <-chan struct{}) error {
	_, err := reader.Read([]byte{0})
	return err
}

func (s *notificationsStateMachine) Close() error {
	return nil
}
