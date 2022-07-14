package sched

import (
	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/common"
	"sync/atomic"
	"time"
)

const maxProcessBatchRows = 500

type ShardScheduler struct {
	shardID                  uint64
	actions                  chan struct{}
	started                  bool
	failed                   bool
	lock                     common.SpinLock
	processorRunning         bool
	forwardRows              []cluster.ForwardRow
	batchHandler             RowsBatchHandler
	lastQueuedReceiverSeq    uint64
	lastProcessedReceiverSeq uint64
}

type RowsBatchHandler interface {
	HandleBatch(shardID uint64, rows []cluster.ForwardRow) error
}

type BatchEntry struct {
	WriteTime time.Time
	Seq       uint32
}

func NewShardScheduler(shardID uint64, batchHandler RowsBatchHandler) *ShardScheduler {
	return &ShardScheduler{
		shardID:      shardID,
		actions:      make(chan struct{}, 10),
		batchHandler: batchHandler,
	}
}

func (s *ShardScheduler) Start() {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.started {
		return
	}
	if len(s.forwardRows) > 0 {
		// There are some rows queued before start waiting to be processed - trigger processing
		s.actions <- struct{}{}
		s.processorRunning = true
	}
	s.started = true
	go s.runLoop()
}

func (s *ShardScheduler) Stop() {
	s.lock.Lock()
	defer s.lock.Unlock()
	if !s.started {
		// If already stopped do nothing
		return
	}
	close(s.actions)
	s.started = false
}

func (s *ShardScheduler) GetLag(now time.Time) time.Duration {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.GetLagNoLock(now)
}

func (s *ShardScheduler) GetLagNoLock(now time.Time) time.Duration {
	if len(s.forwardRows) > 0 {
		nowUnix := now.Sub(common.UnixStart)
		writeTime := time.Duration(s.forwardRows[0].WriteTime)
		return nowUnix - writeTime
	}
	return time.Duration(0)
}

func (s *ShardScheduler) AddRows(rows []cluster.ForwardRow) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.forwardRows = append(s.forwardRows, rows...)
	s.lastQueuedReceiverSeq = rows[len(rows)-1].ReceiverSequence
	if !s.started {
		// We allow rows to be queued before the scheduler is started
		return
	}
	if !s.processorRunning {
		s.actions <- struct{}{}
		s.processorRunning = true
	}
}

func (s *ShardScheduler) WaitForProcessingToComplete(ch chan struct{}) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.failed {
		ch <- struct{}{}
		return
	}
	lastQueued := s.lastQueuedReceiverSeq
	go func() {
		start := time.Now()
		for {
			lastProcessed := atomic.LoadUint64(&s.lastProcessedReceiverSeq)
			if lastProcessed == lastQueued {
				ch <- struct{}{}
				return
			}
			time.Sleep(1 * time.Millisecond)
			if time.Now().Sub(start) > 10*time.Second {
				log.Warnf("timed out waiting for shard %d processing to complete", s.shardID)
				ch <- struct{}{}
				return
			}
		}
	}()
}

func (s *ShardScheduler) ShardID() uint64 {
	return s.shardID
}

func (s *ShardScheduler) getRowBatch() ([]cluster.ForwardRow, bool) {
	s.lock.Lock()
	defer s.lock.Unlock()

	numRows := len(s.forwardRows)
	if numRows > maxProcessBatchRows {
		numRows = maxProcessBatchRows
	}
	rows := s.forwardRows[:numRows]
	s.forwardRows = s.forwardRows[numRows:]
	more := len(s.forwardRows) > 0
	if !more {
		s.processorRunning = false
	}
	return rows, more
}

func (s *ShardScheduler) runLoop() {
	for {
		_, ok := <-s.actions
		if !ok {
			break
		}
		rows, more := s.getRowBatch()
		if len(rows) == 0 {
			panic("expected rows")
		}
		if err := s.batchHandler.HandleBatch(s.shardID, rows); err != nil {
			log.Errorf("failed to process batch: %+v", err)
			s.failed = true
			return
		}
		atomic.StoreUint64(&s.lastProcessedReceiverSeq, rows[len(rows)-1].ReceiverSequence)
		if more {
			s.actions <- struct{}{}
		}
	}
}
