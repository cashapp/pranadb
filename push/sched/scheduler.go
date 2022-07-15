package sched

import (
	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/common"
	"sync"
	"sync/atomic"
	"time"
)

const maxProcessBatchRows = 500

type ShardScheduler struct {
	shardID                  uint64
	actions                  chan struct{}
	started                  bool
	failed                   common.AtomicBool
	lock                     common.SpinLock
	processorRunning         bool
	forwardRows              []cluster.ForwardRow
	batchHandler             RowsBatchHandler
	lastQueuedReceiverSeq    uint64
	lastProcessedReceiverSeq uint64
	stopped                  bool
	loopExitWaitGroup        sync.WaitGroup
}

type RowsBatchHandler interface {
	HandleBatch(shardID uint64, rows []cluster.ForwardRow, first bool) (int64, error)
}

type BatchEntry struct {
	WriteTime time.Time
	Seq       uint32
}

func NewShardScheduler(shardID uint64, batchHandler RowsBatchHandler) *ShardScheduler {
	ss := &ShardScheduler{
		shardID:      shardID,
		actions:      make(chan struct{}, 1),
		batchHandler: batchHandler,
	}
	ss.loopExitWaitGroup.Add(1)
	return ss
}

func (s *ShardScheduler) AddRows(rows []cluster.ForwardRow) {
	s.lock.Lock()
	defer s.lock.Unlock()

	// If the scheduler has been started but now stopped (shutdown) then we just ignore any rows, any incoming will be persisted
	// in the receiver table for next time
	if s.stopped {
		return
	}

	// Sanity check
	if s.lastQueuedReceiverSeq != 0 {
		if s.lastQueuedReceiverSeq+1 != rows[0].ReceiverSequence {
			panic("non contiguous receiver sequence")
		}

		for i, row := range rows {
			if row.ReceiverSequence != s.lastQueuedReceiverSeq+uint64(i)+1 {
				panic("non contiguous rows being added")
			}
		}
	}

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
	defer s.loopExitWaitGroup.Done()
	//var prevSeq int64 = -1
	first := true
	for {
		_, ok := <-s.actions
		if !ok {
			break
		}
		rows, more := s.getRowBatch()
		//if !first && len(rows) == 0 {
		//	panic("expected rows")
		//}

		// It's possible we might get rows with receiverSequence <= lastProcessedSequence
		// This can happen on startup, where the first call in here triggers loading of rows directly from receiver table
		// This can result in also loading rows which were added after start, so we just ignore these extra rows as
		// we've already loaded them
		i := 0
		for _, row := range rows {
			if row.ReceiverSequence > s.lastProcessedReceiverSeq {
				break
			}
			i++
		}
		if i > 0 {
			rows = rows[i:]
		}

		if first || len(rows) > 0 {
			//start := time.Now()
			lastSequence, err := s.batchHandler.HandleBatch(s.shardID, rows, first)
			if err != nil {
				log.Errorf("failed to process batch: %+v", err)
				s.failed.Set(true)
				return
			}
			first = false
			//end := time.Now()

			//log.Printf("shard %d processed %d rows in %d ms current lag is %d ms", s.shardID, len(rows),
			//	end.Sub(start).Milliseconds(), s.GetLag(end).Milliseconds())
			if lastSequence != -1 { // -1 represents no rows returned
				atomic.StoreUint64(&s.lastProcessedReceiverSeq, uint64(lastSequence))
			}
		}

		s.lock.Lock()
		if !s.stopped && more {
			s.actions <- struct{}{}
		}
		s.lock.Unlock()
	}
}

func (s *ShardScheduler) Start() {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.started {
		return
	}
	// We trigger an action on start - there may have been rows queued before we started or pending rows from a previous
	// run that need to be processed
	s.actions <- struct{}{}
	s.processorRunning = true
	s.started = true
	go s.runLoop()
}

func (s *ShardScheduler) Stop() {
	s.lock.Lock()
	if !s.started {
		// If already stopped do nothing
		s.lock.Unlock()
		return
	}
	close(s.actions)
	s.started = false
	s.stopped = true
	s.lock.Unlock()
	// We want to make sure the runLoop has exited before we complete Stop() this means all current batch processing
	// is complete
	s.loopExitWaitGroup.Wait()
}

func (s *ShardScheduler) GetLag(now time.Time) time.Duration {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.getLagNoLock(now)
}

func (s *ShardScheduler) getLagNoLock(now time.Time) time.Duration {
	if len(s.forwardRows) > 0 {
		nowUnix := now.Sub(common.UnixStart)
		writeTime := time.Duration(s.forwardRows[0].WriteTime)
		return nowUnix - writeTime
	}
	return time.Duration(0)
}

func (s *ShardScheduler) WaitForProcessingToComplete(ch chan struct{}) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.failed.Get() {
		ch <- struct{}{}
		return
	}
	lastQueued := s.lastQueuedReceiverSeq
	go func() {
		start := time.Now()
		for {
			lastProcessed := atomic.LoadUint64(&s.lastProcessedReceiverSeq)
			if lastProcessed >= lastQueued {
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
