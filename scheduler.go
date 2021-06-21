package pranadb

import (
	"github.com/squareup/pranadb/storage"
	"log"
)

type ShardScheduler struct {
	shardID uint64
	mover   *Mover
	storage storage.Storage
	actions chan *actionHolder
}

type Action func() error

type actionHolder struct {
	action  Action
	errChan chan error
}

func NewShardScheduler(shardID uint64, mover *Mover, storage storage.Storage) *ShardScheduler {
	return &ShardScheduler{
		shardID: shardID,
		mover:   mover,
		storage: storage,
		actions: make(chan *actionHolder),
	}
}

func (s *ShardScheduler) Start() {
	go s.runLoop()
}

func (s *ShardScheduler) Stop() {
	close(s.actions)
}

func (s *ShardScheduler) runLoop() {
	for {
		holder, ok := <-s.actions
		if !ok {
			break
		}
		holder.errChan <- holder.action()
	}
}

func (s *ShardScheduler) CheckForRemoteBatch() {
	s.ScheduleAction(s.maybeHandleRemoteBatch)
}

func (s *ShardScheduler) ScheduleAction(action Action) chan error {
	ch := make(chan error, 1)
	s.actions <- &actionHolder{
		action:  action,
		errChan: ch,
	}
	return ch
}

func (s *ShardScheduler) maybeHandleRemoteBatch() error {
	log.Printf("In maybeHandleRemoteBatch on shard %d", s.shardID)
	batch := storage.NewWriteBatch(s.shardID)
	err := s.mover.HandleReceivedRows(s.shardID, batch)
	if err != nil {
		return err
	}
	err = s.storage.WriteBatch(batch, true)
	if err != nil {
		return err
	}
	return s.mover.pollForForwards(s.shardID)
}
