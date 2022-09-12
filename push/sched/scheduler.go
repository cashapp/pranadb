package sched

import (
	"bytes"
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

type scheduleChoice int

const (
	scheduleChoiceNone         scheduleChoice = 0
	scheduleChoiceFillBatch    scheduleChoice = 1
	scheduleChoiceProcessBatch scheduleChoice = 2
	scheduleChoiceAction       scheduleChoice = 3
	scheduleChoiceForwardBatch scheduleChoice = 4
)

const maxFillRun = 4

type ShardScheduler struct {
	shardID                      uint64
	processChannel               chan struct{}
	started                      bool
	failed                       bool
	lock                         common.SpinLock
	processorRunning             bool
	forwardRows                  []cluster.ForwardRow
	fillForwardRows              []cluster.ForwardRow
	forwardWrites                []WriteBatchEntry
	actions                      []actionEntry
	queuedWriteRows              int
	queuedActionWeights          int
	batchHandler                 RowsBatchHandler
	ShardFailListener            ShardFailListener
	lastQueuedReceiverSeq        uint64
	lastProcessedReceiverSeq     uint64
	stopped                      bool
	loopExitWaitGroup            sync.WaitGroup
	clust                        cluster.Cluster
	maxProcessBatchSize          int
	maxForwardWriteBatchSize     int
	rowsProcessedCounter         metrics.Counter
	shardLagHistogram            metrics.Observer
	batchProcessingTimeHistogram metrics.Observer
	batchSizeHistogram           metrics.Observer
	processLock                  sync.Mutex
	processingPaused             bool
	fillRun                      int
	lastBatchID                  []byte
	lastBatchProcessed           bool
}

type RowsBatchHandler interface {
	HandleBatch(writeBatch *cluster.WriteBatch, rowGetter RowGetter, rows []cluster.ForwardRow, first bool) (int64, error)
}

// ShardFailListener is called when a shard fails - this is only caused for unretryable errors where we need to stop
// processing, and it allows other things (e.g. aggregations) to clear up any in memory state
type ShardFailListener interface {
	ShardFailed(shardID uint64)
}

type BatchEntry struct {
	WriteTime time.Time
	Seq       uint32
}

type WriteBatchEntry struct {
	writeBatch         []byte
	completionChannels []chan error
}

type actionEntry struct {
	action            func() error
	weight            int
	completionChannel chan error
}

type RowGetter interface {
	Get(key []byte) ([]byte, error)
}

func (s *ShardScheduler) Get(key []byte) ([]byte, error) {
	localGet := false
	if s.lastBatchID != nil {
		if s.lastBatchProcessed {
			localGet = true
		} else {
			lastBatch, err := s.clust.GetLastPersistedBatch(s.shardID)
			if err != nil {
				return nil, err
			}
			if bytes.Compare(lastBatch, s.lastBatchID) == 0 {
				s.lastBatchProcessed = true
				localGet = true
			}
		}
	}
	if localGet {
		return s.clust.LocalGet(key)
	}
	return s.clust.LinearizableGet(s.shardID, key)
}

func NewShardScheduler(shardID uint64, batchHandler RowsBatchHandler, shardFailListener ShardFailListener,
	clust cluster.Cluster, maxProcessBatchSize int, maxForwardWriteBatchSize int) *ShardScheduler {
	sShardID := fmt.Sprintf("shard-%04d", shardID)
	rowsProcessedCounter := rowsProcessedVec.WithLabelValues(sShardID)
	shardLagHistogram := shardLagVec.WithLabelValues(sShardID)
	batchProcessingTimeHistogram := batchProcessingTimeVec.WithLabelValues(sShardID)
	batchSizeHistogram := batchSizeVec.WithLabelValues(sShardID)
	ss := &ShardScheduler{
		shardID:                      shardID,
		processChannel:               make(chan struct{}, 1),
		batchHandler:                 batchHandler,
		ShardFailListener:            shardFailListener,
		rowsProcessedCounter:         rowsProcessedCounter,
		shardLagHistogram:            shardLagHistogram,
		batchProcessingTimeHistogram: batchProcessingTimeHistogram,
		batchSizeHistogram:           batchSizeHistogram,
		clust:                        clust,
		maxProcessBatchSize:          maxProcessBatchSize,
		maxForwardWriteBatchSize:     maxForwardWriteBatchSize,
	}
	ss.loopExitWaitGroup.Add(1)
	return ss
}

// AddAction adds an action, defined by a function to be run on the processor. Actions will have priority over
// process batches and forward writes
func (s *ShardScheduler) AddAction(action func() error, weight int) (chan error, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.failed {
		return nil, errors.New("cannot add action, scheduler is failed")
	}
	if s.stopped {
		return nil, errors.NewPranaErrorf(errors.Unavailable, "cannot add action, scheduler has stopped")
	}

	completionChannel := make(chan error, 1)

	s.actions = append(s.actions, actionEntry{
		action:            action,
		weight:            weight,
		completionChannel: completionChannel,
	})

	s.queuedActionWeights += weight

	s.maybeStartRunning()
	return completionChannel, nil
}

func (s *ShardScheduler) AddForwardBatch(writeBatch []byte) error {
	ch, err := s.addForwardBatch(writeBatch)
	if err != nil {
		return err
	}
	return <-ch
}

func (s *ShardScheduler) addForwardBatch(writeBatch []byte) (chan error, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	ch := make(chan error, 1)

	if s.failed {
		return nil, errors.New("cannot add forward batch, scheduler is failed")
	}
	if s.stopped {
		return nil, errors.NewPranaErrorf(errors.Unavailable, "cannot add forward batch, scheduler has stopped")
	}

	s.forwardWrites = append(s.forwardWrites, WriteBatchEntry{
		writeBatch:         writeBatch,
		completionChannels: []chan error{ch},
	})

	numPuts := getNumPuts(writeBatch)
	log.Tracef("scheduler %d received forward write batch of size %d", s.shardID, numPuts)
	s.queuedWriteRows += int(numPuts)

	s.maybeStartRunning()
	return ch, nil
}

func (s *ShardScheduler) AddRows(rows []cluster.ForwardRow) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.failed {
		return
	}
	if s.stopped {
		return
	}

	for _, row := range rows {
		if row.Fill {
			// We put fill batches in a separate queue and prioritise them
			s.fillForwardRows = append(s.fillForwardRows, row)
		} else {
			s.forwardRows = append(s.forwardRows, row)
		}
	}
	if len(s.forwardRows) > 0 {
		s.lastQueuedReceiverSeq = s.forwardRows[len(s.forwardRows)-1].ReceiverSequence
	}
	s.maybeStartRunning()
}

func (s *ShardScheduler) PauseProcessing() {
	// We need to get the process lock first - this ensures no processing is occurring and no more process batches are
	// waiting to be processed once we obtain it
	s.processLock.Lock()
	// Then we get the scheduler lock and set the processing paused flag
	// this ensures no more process batches will be returned from getNextBatch until it's resumed
	s.lock.Lock()
	s.processingPaused = true
	s.lock.Unlock()
	s.processLock.Unlock()
}

func (s *ShardScheduler) ResumeProcessing() {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.processingPaused = false
	s.maybeStartRunning()
}

func (s *ShardScheduler) getNextBatch() ([]cluster.ForwardRow, *WriteBatchEntry, *actionEntry, bool) { //nolint:gocyclo
	s.lock.Lock()
	defer s.lock.Unlock()

	numActions := s.queuedActionWeights
	numToProcess := len(s.forwardRows)
	numFillsToProcess := len(s.fillForwardRows)
	numForwardWrites := s.queuedWriteRows

	var rowsToProcess []cluster.ForwardRow
	var forwardWriteBatch *WriteBatchEntry
	var action *actionEntry

	// We choose what kind of work to process

	choice := scheduleChoiceNone

	if !s.processingPaused && (numToProcess > numActions || numToProcess > numForwardWrites) {
		// In usual operation we prioritise process batches - this is because both actions and forward writes can create new
		// process batches. If we prioritised actions or forward writes we could end up with process batches
		// building up without bound
		choice = scheduleChoiceProcessBatch
	} else if numActions > 0 {
		// Next, we prioritise actions - this is so MV fills get high priority
		choice = scheduleChoiceAction
	} else if numForwardWrites > 0 {
		// And finally we choose forward writes
		choice = scheduleChoiceForwardBatch
	}

	if numFillsToProcess > 0 && (s.fillRun < maxFillRun || choice == scheduleChoiceNone) {
		// However fill batches, if any, get the highest priority, so we don't starve out fill under high ingest.
		// But we also don't want long fill runs to starve out any ingest, so we limit consecutive run of fill batches
		// to maxFillRun
		choice = scheduleChoiceFillBatch
	}

	// Sanity check
	if choice == scheduleChoiceNone && (numActions > 0 || numForwardWrites > 0 || numFillsToProcess > 0 || (!s.processingPaused && numToProcess > 0)) {
		panic("work to process but none chosen")
	}

	switch choice {
	case scheduleChoiceFillBatch:
		rowsToProcess = s.getFillRowsToProcess()
		s.fillRun++
	case scheduleChoiceProcessBatch:
		rowsToProcess = s.getRowsToProcess()
		s.fillRun = 0
	case scheduleChoiceAction:
		action = s.getAction()
	case scheduleChoiceForwardBatch:
		forwardWriteBatch = s.getForwardWriteBatch()
		s.fillRun = 0
	default:
	}

	more := s.queuedWriteRows > 0 || len(s.actions) > 0 || len(s.fillForwardRows) > 0
	if !s.processingPaused {
		more = more || len(s.forwardRows) > 0
	}

	if !more {
		s.processorRunning = false
	}
	return rowsToProcess, forwardWriteBatch, action, more
}

func (s *ShardScheduler) getAction() *actionEntry {
	entry := s.actions[0]
	s.actions = s.actions[1:]
	s.queuedActionWeights -= entry.weight
	return &entry
}

func (s *ShardScheduler) getRowsToProcess() []cluster.ForwardRow {
	numRows := len(s.forwardRows)
	if numRows > s.maxProcessBatchSize {
		numRows = s.maxProcessBatchSize
	}
	rows := s.forwardRows[:numRows]
	s.forwardRows = s.forwardRows[numRows:]
	return rows
}

func (s *ShardScheduler) getFillRowsToProcess() []cluster.ForwardRow {
	numRows := len(s.fillForwardRows)
	if numRows > s.maxProcessBatchSize {
		numRows = s.maxProcessBatchSize
	}
	rows := s.fillForwardRows[:numRows]
	s.fillForwardRows = s.fillForwardRows[numRows:]
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
			combinedEntries = append(combinedEntries, batch.writeBatch[3:]...)
			entries += int(numPuts)
			completionChannels = append(completionChannels, batch.completionChannels[0])
			if entries >= s.maxForwardWriteBatchSize || nextBatchIndex == 255 {
				break
			}
		}
		bigBatch := make([]byte, 0, len(combinedEntries)+3)
		bigBatch = append(bigBatch, s.forwardWrites[0].writeBatch[0]) // shardStateMachineCommandForwardWrite
		bigBatch = append(bigBatch, s.forwardWrites[0].writeBatch[1]) // 0 - not a fill
		bigBatch = append(bigBatch, byte(nextBatchIndex))             // number of batches in the write
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

func getNumPuts(batch []byte) uint32 {
	numPuts, _ := common.ReadUint32FromBufferLE(batch, 3)
	return numPuts
}

func (s *ShardScheduler) maybeStartRunning() {
	if s.started && !s.processorRunning {
		s.processChannel <- struct{}{}
		s.processorRunning = true
	}
}

func (s *ShardScheduler) innerLoop(first bool) (bool, bool) {
	_, ok := <-s.processChannel
	if !ok {
		return false, false
	}

	s.processLock.Lock()
	defer s.processLock.Unlock()

	var more bool
	if first {
		// This will trigger loading any old rows from the receiver table
		if !s.processBatch(nil, true, false) {
			return false, false
		}
		more = true // trigger another one
	} else {

		rowsToProcess, writeBatch, action, m := s.getNextBatch()
		more = m

		lr := len(rowsToProcess)

		if writeBatch != nil && lr > 0 {
			// Sanity check
			panic("got writes and rows to process")
		}

		if action != nil {
			err := action.action()
			action.completionChannel <- err
		} else if lr > 0 {

			var fill bool
			if len(rowsToProcess) > 0 {
				fill = rowsToProcess[0].Fill
			}
			// It's possible we might get rows with receiverSequence <= lastProcessedSequence
			// This can happen on startup, where the first call in here triggers loading of rows directly from receiver table
			// This can result in also loading rows which were added after start, so we just ignore these extra rows as
			// we've already loaded them
			if !fill {
				// We don't count fill batches as they come out of order to non fill batches
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
			}

			lr = len(rowsToProcess)
			if lr > 0 {
				start := common.NanoTime()
				if !s.processBatch(rowsToProcess, false, fill) {
					return false, false
				}
				processTime := common.NanoTime() - start
				log.Tracef("processed batch of %d rows in %d ms for shard %d", lr, processTime/1000000, s.shardID)
				s.batchProcessingTimeHistogram.Observe(float64(processTime / 1000000))
				s.rowsProcessedCounter.Add(float64(lr))
				s.batchSizeHistogram.Observe(float64(lr))
			}
		} else if writeBatch != nil {
			start := common.NanoTime()
			err := s.clust.ExecuteForwardBatch(s.shardID, writeBatch.writeBatch)
			processTime := common.NanoTime() - start
			log.Tracef("wrote forward rows: %d rows in %d ms", getNumPuts(writeBatch.writeBatch), processTime/1000000)
			for _, ch := range writeBatch.completionChannels {
				ch <- err
			}
			if err != nil {
				log.Errorf("failed to execute forward write batch: %+v", err)
				s.setFailed(err)
				return false, false
			}
		}
	}
	return true, more
}

func (s *ShardScheduler) runLoop() {
	defer s.loopExitWaitGroup.Done()
	first := true
	for {
		ok, more := s.innerLoop(first)
		if !ok {
			return
		}
		s.lock.Lock()
		lag := s.getLagNoLock(common.NanoTime())
		if !s.stopped && more {
			s.processChannel <- struct{}{}
		}
		s.lock.Unlock()
		s.shardLagHistogram.Observe(float64(lag.Milliseconds()))
		first = false
	}
}

func (s *ShardScheduler) processBatch(rowsToProcess []cluster.ForwardRow, first bool, fill bool) bool {
	writeBatch := cluster.NewWriteBatch(s.shardID)
	lastSequence, err := s.batchHandler.HandleBatch(writeBatch, s, rowsToProcess, first)
	if err != nil {
		s.setFailed(err)
		return false
	}
	if !fill && lastSequence != -1 { // -1 represents no rows returned
		atomic.StoreUint64(&s.lastProcessedReceiverSeq, uint64(lastSequence))
	}
	if writeBatch.BatchID != nil {
		s.lastBatchID = writeBatch.BatchID
		s.lastBatchProcessed = false
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
	s.processChannel <- struct{}{}
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
	close(s.processChannel)
	s.started = false
	s.stopped = true
	s.processorRunning = false
	if !s.failed {
		s.unblockWaitingWrites(errors.New("scheduler is closed"))
	}
	s.lock.Unlock()
	// We want to make sure the runLoop has exited before we complete Stop() this means all current batch processing
	// is complete
	s.loopExitWaitGroup.Wait()
}

func (s *ShardScheduler) isStopped() bool {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.stopped
}

// called on unretryable error where we must stop processing
func (s *ShardScheduler) setFailed(err error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	log.Tracef("scheduler %d set failed %v", s.shardID, err)
	if s.failed || s.stopped {
		return
	}
	s.stopped = true
	s.failed = true
	s.ShardFailListener.ShardFailed(s.shardID) // clean up state
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

// WaitForCurrentProcessingToComplete waits for any batches queued for processing to complete
// it does not wait for fill batches or for any other batches that arrive after it is called
func (s *ShardScheduler) WaitForCurrentProcessingToComplete(ch chan struct{}) {
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
			if lastProcessed >= lastQueued {
				ch <- struct{}{}
				return
			}
			time.Sleep(1 * time.Millisecond)
			// This timeout must be larger than 2 * callTimeout in dragon - as requests can be retried and they will block the processor
			if time.Now().Sub(start) > 30*time.Second {
				s.lock.Lock()
				defer s.lock.Unlock()
				log.Warnf("timed out waiting for shard %d processing to complete queued writes %d last processed %d lastQueued %d",
					s.shardID, s.queuedWriteRows, lastProcessed, lastQueued)
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
