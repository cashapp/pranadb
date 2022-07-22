package sched

import (
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/metrics"
	"sync"
	"sync/atomic"
	"time"
)

const maxProcessBatchRows = 500

var (
	rowsProcessedVec = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pranadb_rows_processed_total",
		Help: "counter for number of rows processed, segmented by shard id",
	}, []string{"shard_id"})
	shardLagVec = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "pranadb_shard_lag",
		Help: "histogram measuring processing lag of a shard in ms",
	}, []string{"shard_id"})
	batchProcessingTimeVec = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "pranadb_batch_process_time",
		Help: "histogram measuring processing time of a batch in ms",
	}, []string{"shard_id"})
	batchSizeVec = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "pranadb_batch_size",
		Help: "histogram measuring size of a batch in rows",
	}, []string{"shard_id"})
)

type ShardScheduler struct {
	shardID                      uint64
	actions                      chan struct{}
	started                      bool
	failed                       common.AtomicBool
	lock                         common.SpinLock
	processorRunning             bool
	forwardRows                  []cluster.ForwardRow
	batchHandler                 RowsBatchHandler
	lastQueuedReceiverSeq        uint64
	lastProcessedReceiverSeq     uint64
	stopped                      bool
	loopExitWaitGroup            sync.WaitGroup
	rowsProcessedCounter         metrics.Counter
	shardLagHistogram            metrics.Observer
	batchProcessingTimeHistogram metrics.Observer
	batchSizeHistogram           metrics.Observer
}

type RowsBatchHandler interface {
	HandleBatch(shardID uint64, rows []cluster.ForwardRow, first bool) (int64, error)
}

type BatchEntry struct {
	WriteTime time.Time
	Seq       uint32
}

func NewShardScheduler(shardID uint64, batchHandler RowsBatchHandler) *ShardScheduler {
	sShardID := fmt.Sprintf("shard-%04d", shardID)
	rowsProcessedCounter := rowsProcessedVec.WithLabelValues(sShardID)
	shardLagHistogram := shardLagVec.WithLabelValues(sShardID)
	batchProcessingTimeHistogram := batchProcessingTimeVec.WithLabelValues(sShardID)
	batchSizeHistogram := batchSizeVec.WithLabelValues(sShardID)
	ss := &ShardScheduler{
		shardID:                      shardID,
		actions:                      make(chan struct{}, 1),
		batchHandler:                 batchHandler,
		rowsProcessedCounter:         rowsProcessedCounter,
		shardLagHistogram:            shardLagHistogram,
		batchProcessingTimeHistogram: batchProcessingTimeHistogram,
		batchSizeHistogram:           batchSizeHistogram,
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

		lr := len(rows)

		if first || lr > 0 {

			start := common.NanoTime()
			lastSequence, err := s.batchHandler.HandleBatch(s.shardID, rows, first)
			if err != nil {
				log.Errorf("failed to process batch: %+v", err)
				s.failed.Set(true)
				return
			}
			processTime := common.NanoTime() - start

			s.batchProcessingTimeHistogram.Observe(float64(processTime / 1000000))
			s.rowsProcessedCounter.Add(float64(lr))
			s.batchSizeHistogram.Observe(float64(lr))

			first = false

			if lastSequence != -1 { // -1 represents no rows returned
				atomic.StoreUint64(&s.lastProcessedReceiverSeq, uint64(lastSequence))
			}

		}

		s.lock.Lock()
		lag := s.getLagNoLock(common.NanoTime())
		if !s.stopped && more {
			s.actions <- struct{}{}
		}
		s.lock.Unlock()
		s.shardLagHistogram.Observe(float64(lag.Milliseconds()))
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

func (s *ShardScheduler) GetLag(nowNanos uint64) time.Duration {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.getLagNoLock(nowNanos)
}

func (s *ShardScheduler) getLagNoLock(nowNanos uint64) time.Duration {
	if len(s.forwardRows) > 0 {
		return time.Duration(nowNanos - s.forwardRows[0].WriteTime)
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
