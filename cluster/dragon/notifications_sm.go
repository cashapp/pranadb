package dragon

import (
	"io"

	"github.com/lni/dragonboat/v3/statemachine"
	"github.com/pkg/errors"

	"github.com/squareup/pranadb/cluster"
)

const (
	notificationsStateMachineUpdatedOK uint64 = 1
)

func (d *Dragon) newNotificationsStateMachine(_ uint64, _ uint64) statemachine.IStateMachine {
	return &notificationsStateMachine{
		dragon: d,
	}
}

type notificationsStateMachine struct {
	dragon *Dragon
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
	// TODO
	return nil
}

func (s *notificationsStateMachine) RecoverFromSnapshot(reader io.Reader, files []statemachine.SnapshotFile, i <-chan struct{}) error {
	// TODO
	return nil
}

func (s *notificationsStateMachine) Close() error {
	return nil
}
