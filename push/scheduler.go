package push

import (
	"log"
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
		actions: make(chan *actionHolder, 100), // TODO make configurable
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
		err := holder.action()
		if holder.errChan != nil {
			holder.errChan <- err
		} else if err != nil {
			log.Printf("Failed to execute action: %v", err)
		}
	}
}

func (s *shardScheduler) CheckForRemoteBatch() {
	s.ScheduleActionFireAndForget(s.maybeHandleRemoteBatch)
}

func (s *shardScheduler) CheckForRowsToForward() chan error {
	return s.ScheduleAction(s.maybeForwardRows)
}

func (s *shardScheduler) ScheduleAction(action Action) chan error {
	ch := make(chan error)
	s.actions <- &actionHolder{
		action:  action,
		errChan: ch,
	}
	return ch
}

func (s *shardScheduler) ScheduleActionFireAndForget(action Action) {
	s.actions <- &actionHolder{
		action: action,
	}
}

func (s *shardScheduler) maybeHandleRemoteBatch() error {
	log.Printf("In maybeHandleRemoteBatch on shard %d", s.shardID)
	err := s.engine.handleReceivedRows(s.shardID, s.engine)
	if err != nil {
		return err
	}
	return s.engine.transferData(s.shardID, true)
}

func (s *shardScheduler) maybeForwardRows() error {
	return s.engine.transferData(s.shardID, true)
}
