package sched

import (
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/metrics"
	"sync"
	"sync/atomic"
	"time"
)

const maxProcessBatchRows = 2000
const maxForwardWriteBatchSize = 500
const addForwardRowsTimeout = 1 * time.Minute

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
	failed                       bool
	lock                         common.SpinLock
	processorRunning             bool
	forwardRows                  []cluster.ForwardRow
	forwardWrites                []WriteBatchEntry
	batchHandler                 RowsBatchHandler
	lastQueuedReceiverSeq        uint64
	lastProcessedReceiverSeq     uint64
	stopped                      bool
	loopExitWaitGroup            sync.WaitGroup
	rowsProcessedCounter         metrics.Counter
	shardLagHistogram            metrics.Observer
	batchProcessingTimeHistogram metrics.Observer
	batchSizeHistogram           metrics.Observer
	queuedWriteRows              int
	clust                        cluster.Cluster
}

type RowsBatchHandler interface {
	HandleBatch(shardID uint64, rows []cluster.ForwardRow, first bool) (int64, error)
}

type BatchEntry struct {
	WriteTime time.Time
	Seq       uint32
}

type WriteBatchEntry struct {
	writeBatch         []byte
	completionChannels []chan error
}

func NewShardScheduler(shardID uint64, batchHandler RowsBatchHandler, clust cluster.Cluster) *ShardScheduler {
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
		clust:                        clust,
	}
	ss.loopExitWaitGroup.Add(1)
	return ss
}

func (s *ShardScheduler) AddForwardBatch(writeBatch []byte) error {
	ch, err := s.addForwardBatch(writeBatch)
	if err != nil {
		return err
	}
	select {
	case err := <-ch:
		return err
	case <-time.After(addForwardRowsTimeout):
		return errors.NewPranaErrorf(errors.Timeout, "timed out in waiting for forward write batch to be processed for shard %d", s.shardID)
	}
}

func getNumPuts(batch []byte) uint32 {
	numPuts, _ := common.ReadUint32FromBufferLE(batch, 1)
	return numPuts
}

func (s *ShardScheduler) addForwardBatch(writeBatch []byte) (chan error, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	ch := make(chan error)

	if s.stopped {
		return nil, errors.New("cannot add forward batch, scheduler is stopped")
	}

	s.forwardWrites = append(s.forwardWrites, WriteBatchEntry{
		writeBatch:         writeBatch,
		completionChannels: []chan error{ch},
	})

	numPuts := getNumPuts(writeBatch)
	log.Infof("scheduler %d receiver forward write batch of size %d", s.shardID, numPuts)
	s.queuedWriteRows += int(numPuts)

	if !s.started {
		// We allow rows to be queued before the scheduler is started
		return ch, nil
	}
	s.maybeStartRunning()
	return ch, nil
}

func (s *ShardScheduler) maybeStartRunning() {
	if !s.processorRunning {
		s.actions <- struct{}{}
		s.processorRunning = true
	}
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
	}

	s.forwardRows = append(s.forwardRows, rows...)
	s.lastQueuedReceiverSeq = rows[len(rows)-1].ReceiverSequence
	if !s.started {
		// We allow rows to be queued before the scheduler is started
		return
	}
	s.maybeStartRunning()
}

func (s *ShardScheduler) getNextBatch() ([]cluster.ForwardRow, *WriteBatchEntry, bool) {
	s.lock.Lock()
	defer s.lock.Unlock()

	numToProcess := len(s.forwardRows)

	var rowsToProcess []cluster.ForwardRow
	var forwardWriteBatch *WriteBatchEntry

	if numToProcess >= s.queuedWriteRows {
		rowsToProcess = s.getRowsToProcess()
	} else if s.queuedWriteRows > numToProcess {
		forwardWriteBatch = s.getForwardWriteBatch()
	}

	more := len(s.forwardRows) > 0 || s.queuedWriteRows > 0
	if !more {
		s.processorRunning = false
	}
	return rowsToProcess, forwardWriteBatch, more
}

func (s *ShardScheduler) getRowsToProcess() []cluster.ForwardRow {
	numRows := len(s.forwardRows)
	if numRows > maxProcessBatchRows {
		numRows = maxProcessBatchRows
	}
	rows := s.forwardRows[:numRows]
	s.forwardRows = s.forwardRows[numRows:]
	return rows
}

func (s *ShardScheduler) getForwardWriteBatch() *WriteBatchEntry {
	if len(s.forwardWrites) != 1 {
		// We combine the write batches into a single batch
		combinedEntries := make([]byte, 0, len(s.forwardWrites[0].writeBatch)*2)
		entries := 0
		nextBatchIndex := 0
		var completionChannels []chan error
		for _, batch := range s.forwardWrites {
			nextBatchIndex++
			numPuts := getNumPuts(batch.writeBatch)
			combinedEntries = append(combinedEntries, batch.writeBatch[9:]...)
			entries += int(numPuts)
			completionChannels = append(completionChannels, batch.completionChannels[0])
			if entries >= maxForwardWriteBatchSize {
				break
			}
		}
		bigBatch := make([]byte, 0, len(combinedEntries)+9)
		bigBatch = append(bigBatch, s.forwardWrites[0].writeBatch[0])
		bigBatch = common.AppendUint32ToBufferLE(bigBatch, uint32(entries))
		bigBatch = common.AppendUint32ToBufferLE(bigBatch, 0)
		bigBatch = append(bigBatch, combinedEntries...)
		s.forwardWrites = s.forwardWrites[nextBatchIndex:]
		s.queuedWriteRows -= entries
		return &WriteBatchEntry{
			writeBatch:         bigBatch,
			completionChannels: completionChannels,
		}
	}
	batch := s.forwardWrites[0]
	s.forwardWrites = s.forwardWrites[1:]
	s.queuedWriteRows -= int(getNumPuts(batch.writeBatch))
	return &batch
}

func (s *ShardScheduler) runLoop() {
	defer s.loopExitWaitGroup.Done()
	first := true
	for {
		_, ok := <-s.actions
		if !ok {
			break
		}

		var more bool
		if first {
			// This will trigger loading any old rows from the receiver table
			if !s.processBatch(nil, true) {
				return
			}
			more = true // trigger another one
			first = false
		} else {

			rowsToProcess, writeBatch, m := s.getNextBatch()
			more = m

			lr := len(rowsToProcess)

			if writeBatch != nil && lr > 0 {
				// Sanity check
				panic("got writes and rows to process")
			}

			if lr > 0 {

				// The first time we always trigger a process even if there are no rows in the scheduler in order
				// to process any rows in the receiver table at startup

				// It's possible we might get rowsToProcess with receiverSequence <= lastProcessedSequence
				// This can happen on startup, where the first call in here triggers loading of rowsToProcess directly from receiver table
				// This can result in also loading rowsToProcess which were added after start, so we just ignore these extra rowsToProcess as
				// we've already loaded them
				i := 0
				for _, row := range rowsToProcess {
					if row.ReceiverSequence > s.lastProcessedReceiverSeq {
						break
					}
					i++
				}
				if i > 0 {
					rowsToProcess = rowsToProcess[i:]
				}
				lr = len(rowsToProcess)
				if lr > 0 {
					start := common.NanoTime()
					if !s.processBatch(rowsToProcess, false) {
						return
					}
					processTime := common.NanoTime() - start

					log.Infof("processed batch of %d rows in %d ms", lr, processTime/1000000)

					s.batchProcessingTimeHistogram.Observe(float64(processTime / 1000000))
					s.rowsProcessedCounter.Add(float64(lr))
					s.batchSizeHistogram.Observe(float64(lr))
				}
			} else if writeBatch != nil {
				start := common.NanoTime()
				err := s.clust.ExecuteForwardBatch(s.shardID, writeBatch.writeBatch)
				processTime := common.NanoTime() - start
				log.Infof("wrote forward rows: %d rows in %d ms", getNumPuts(writeBatch.writeBatch), processTime/1000000)
				for _, ch := range writeBatch.completionChannels {
					ch <- err
				}
				if err != nil {
					log.Errorf("failed to execute forward write batch: %+v", err)
					s.setFailed(err)
					return
				}
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

func (s *ShardScheduler) processBatch(rowsToProcess []cluster.ForwardRow, first bool) bool {
	lastSequence, err := s.batchHandler.HandleBatch(s.shardID, rowsToProcess, first)
	if err != nil {
		log.Errorf("failed to process batch: %+v", err)
		s.setFailed(err)
		return false
	}
	if lastSequence != -1 { // -1 represents no rowsToProcess returned
		atomic.StoreUint64(&s.lastProcessedReceiverSeq, uint64(lastSequence))
	}
	return true
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
	if !s.failed {
		s.unblockWaitingWrites(errors.New("scheduler is closed"))
	}
	s.lock.Unlock()
	// We want to make sure the runLoop has exited before we complete Stop() this means all current batch processing
	// is complete
	s.loopExitWaitGroup.Wait()
}

func (s *ShardScheduler) setFailed(err error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.failed || s.stopped {
		return
	}
	s.stopped = true
	s.failed = true
	// We unblock all waiting writes
	s.unblockWaitingWrites(err)
	s.processorRunning = false
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
	if s.failed {
		ch <- struct{}{}
		return
	}
	lastQueued := s.lastQueuedReceiverSeq
	go func() {
		start := time.Now()
		for {
			lastProcessed := atomic.LoadUint64(&s.lastProcessedReceiverSeq)
			s.lock.Lock()
			queuedWrites := s.queuedWriteRows
			s.lock.Unlock()
			if lastProcessed >= lastQueued && queuedWrites == 0 {
				ch <- struct{}{}
				return
			}
			time.Sleep(1 * time.Millisecond)
			if time.Now().Sub(start) > 5*time.Second {
				s.lock.Lock()
				defer s.lock.Unlock()
				log.Warnf("timed out waiting for shard %d processing to complete", s.shardID)
				ch <- struct{}{}
				return
			}
		}
	}()
}

func (s *ShardScheduler) unblockWaitingWrites(err error) {
	for _, fw := range s.forwardWrites {
		fw.completionChannels[0] <- err
	}
}

func (s *ShardScheduler) ShardID() uint64 {
	return s.shardID
}
