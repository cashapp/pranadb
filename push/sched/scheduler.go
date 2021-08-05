package sched

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

var runningSchedulers int32

type ShardScheduler struct {
	shardID  uint64
	actions  chan *actionHolder
	stopLock sync.Mutex
	stopped  bool
}

type Action func() error

type actionHolder struct {
	action  Action
	errChan chan error
}

func NewShardScheduler(shardID uint64) *ShardScheduler {
	return &ShardScheduler{
		shardID: shardID,
		actions: make(chan *actionHolder, 100), // TODO make configurable
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
	s.incRunning()
	defer s.decRunning()
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

func (s *ShardScheduler) incRunning() {
	atomic.AddInt32(&runningSchedulers, 1)
}

func (s *ShardScheduler) decRunning() {
	atomic.AddInt32(&runningSchedulers, -1)
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

func WaitUntilNoSchedulersRunning() error {
	start := time.Now()
	for {
		numSchedulers := atomic.LoadInt32(&runningSchedulers)
		if numSchedulers == 0 {
			return nil
		}
		time.Sleep(time.Millisecond)
		if time.Now().Sub(start) >= 5*time.Second {
			return fmt.Errorf("timed out waiting for schedulers to stop running, sched count is %d", numSchedulers)
		}
	}
}
