package sched

import (
	"sync"

	"go.uber.org/zap"
)

type ShardScheduler struct {
	shardID  uint64
	actions  chan *actionHolder
	stopLock sync.Mutex
	stopped  bool
	logger   *zap.Logger
}

type Action func() error

type actionHolder struct {
	action  Action
	errChan chan error
}

func NewShardScheduler(shardID uint64, logger *zap.Logger) *ShardScheduler {
	return &ShardScheduler{
		shardID: shardID,
		actions: make(chan *actionHolder, 100), // TODO make configurable
		logger:  logger,
	}
}

func (s *ShardScheduler) Start() {
	go s.runLoop()
}

func (s *ShardScheduler) Stop() {
	s.stopLock.Lock()
	defer s.stopLock.Unlock()
	if s.stopped {
		// If already stopped do nothing
		return
	}
	s.stopped = true
	close(s.actions)
}

func (s *ShardScheduler) runLoop() {
	for {
		holder, ok := <-s.actions
		if !ok {
			break
		}
		err := holder.action()
		if holder.errChan != nil {
			holder.errChan <- err
		} else if err != nil {
			s.logger.Error("Failed to execute action", zap.Error(err))
		}
	}
}

func (s *ShardScheduler) ScheduleAction(action Action) chan error {
	// Channel size is 1 - we don't want writer to block waiting for reader
	ch := make(chan error, 1)
	s.actions <- &actionHolder{
		action:  action,
		errChan: ch,
	}
	return ch
}

func (s *ShardScheduler) ScheduleActionFireAndForget(action Action) {
	s.actions <- &actionHolder{
		action: action,
	}
}

func (s *ShardScheduler) ShardID() uint64 {
	return s.shardID
}
