package push

import (
	"log"

	"github.com/squareup/pranadb/storage"
)

type shardScheduler struct {
	shardID uint64
	engine  *PushEngine
	actions chan *actionHolder
}

type Action func() error

type actionHolder struct {
	action  Action
	errChan chan error
}

func newShardScheduler(shardID uint64, mover *PushEngine) *shardScheduler {
	return &shardScheduler{
		shardID: shardID,
		engine:  mover,
		actions: make(chan *actionHolder),
	}
}

func (s *shardScheduler) Start() {
	go s.runLoop()
}

func (s *shardScheduler) Stop() {
	close(s.actions)
}

func (s *shardScheduler) runLoop() {
	for {
		holder, ok := <-s.actions
		if !ok {
			break
		}
		holder.errChan <- holder.action()
	}
}

func (s *shardScheduler) CheckForRemoteBatch() {
	s.ScheduleAction(s.maybeHandleRemoteBatch)
}

func (s *shardScheduler) ScheduleAction(action Action) chan error {
	ch := make(chan error, 1)
	s.actions <- &actionHolder{
		action:  action,
		errChan: ch,
	}
	return ch
}

func (s *shardScheduler) maybeHandleRemoteBatch() error {
	log.Printf("In maybeHandleRemoteBatch on shard %d", s.shardID)
	batch := storage.NewWriteBatch(s.shardID)
	err := s.engine.handleReceivedRows(s.shardID, batch)
	if err != nil {
		return err
	}
	err = s.engine.storage.WriteBatch(batch, true)
	if err != nil {
		return err
	}
	return s.engine.pollForForwards(s.shardID)
}
