package sched

import (
	"log"
	"sync"
)

type ShardScheduler struct {
	shardID uint64
	actions chan *actionHolder
	lock    sync.Mutex
	started bool
	paused  bool
}

type Action func() error

type actionHolder struct {
	action  Action
	errChan chan error
	pause   bool
}

func NewShardScheduler(shardID uint64) *ShardScheduler {
	return &ShardScheduler{
		shardID: shardID,
		actions: make(chan *actionHolder, 1000), // TODO make configurable
	}
}

func (s *ShardScheduler) Start() {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.started {
		return
	}
	go s.runLoop()
	s.started = true
}

func (s *ShardScheduler) Stop() {
	s.lock.Lock()
	defer s.lock.Unlock()
	if !s.started {
		// If already stopped do nothing
		return
	}
	s.started = false
	close(s.actions)
}

func (s *ShardScheduler) Pause() {
	s.lock.Lock()
	defer s.lock.Unlock()
	if !s.started {
		return
	}
	if s.paused {
		return
	}
	ch := make(chan error, 1)
	s.actions <- &actionHolder{
		action: func() error {
			return nil
		},
		errChan: ch,
		pause:   true,
	}
	<-ch
	s.paused = true
}

func (s *ShardScheduler) Resume() {
	s.lock.Lock()
	defer s.lock.Unlock()
	if !s.started {
		return
	}
	if !s.paused {
		return
	}
	go s.runLoop()
	s.paused = false
}

func (s *ShardScheduler) runLoop() {
	for {
		holder, ok := <-s.actions
		if !ok {
			break
		}
		if holder.pause {
			holder.errChan <- nil
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
