package push

import (
	"github.com/squareup/pranadb/storage"
	"log"
)

type shardScheduler struct {
	shardID uint64
	mover   *mover
	storage storage.Storage
	actions chan *actionHolder
}

type Action func() error

type actionHolder struct {
	action  Action
	errChan chan error
}

func NewShardScheduler(shardID uint64, mover *mover, storage storage.Storage) *shardScheduler {
	return &shardScheduler{
		shardID: shardID,
		mover:   mover,
		storage: storage,
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
	err := s.mover.HandleReceivedRows(s.shardID, batch)
	if err != nil {
		return err
	}
	err = s.storage.WriteBatch(batch, true)
	if err != nil {
		return err
	}
	return s.mover.PollForForwards(s.shardID)
}
