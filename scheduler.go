package pranadb

import (
	"github.com/squareup/pranadb/storage"
	"log"
)

type ShardScheduler struct {
	shardID uint64
	mover   *Mover
	storage storage.Storage
	trigger chan bool
}

func NewShardScheduler(shardID uint64, mover *Mover, storage storage.Storage) *ShardScheduler {
	return &ShardScheduler{
		shardID: shardID,
		mover:   mover,
		storage: storage,
		trigger: make(chan bool),
	}
}

func (s *ShardScheduler) Start() {
	go s.runLoop()
}

func (s *ShardScheduler) Stop() {
	close(s.trigger)
}

func (s *ShardScheduler) runLoop() {
	for {
		_, ok := <-s.trigger
		if !ok {
			break
		}
		err := s.maybeHandleRemoteBatch()
		if err != nil {
			// TODO best way to log stuff?
			log.Println(err)
			break
		}
	}
}

func (s *ShardScheduler) CheckForRemoteBatch() {
	s.trigger <- true
}

func (s *ShardScheduler) maybeHandleRemoteBatch() error {
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
