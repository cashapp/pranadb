package exec

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/failinject"
	"github.com/squareup/pranadb/interruptor"
	"github.com/squareup/pranadb/push/sched"
	"github.com/squareup/pranadb/push/util"
	"github.com/squareup/pranadb/table"
	"sync"
	"sync/atomic"
	"time"
)

const (
	fillMaxBatchSize   = 500
	lockAndLoadMaxRows = 1000
)

var (
	rowsFilledVec = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pranadb_rows_filled_total",
		Help: "counter for number of rows filled, segmented by source name",
	}, []string{"table_name"})
)

// TableExecutor updates the changes into the associated table - used to persist state
// of a materialized view or source
type TableExecutor struct {
	pushExecutorBase
	TableInfo         *common.TableInfo
	consumingNodes    map[string]PushExecutor
	store             cluster.Cluster
	lock              sync.RWMutex
	filling           bool
	lastSequences     sync.Map
	fillTableID       uint64
	delayer           interruptor.InterruptManager
	transient         bool
	retentionDuration time.Duration
	rowsFilledCounter prometheus.Counter
}

func NewTableExecutor(tableInfo *common.TableInfo, store cluster.Cluster, transient bool, retentionDuration time.Duration) *TableExecutor {
	rowsFilledCounter := rowsFilledVec.WithLabelValues(tableInfo.Name)
	return &TableExecutor{
		pushExecutorBase: pushExecutorBase{
			colNames:    tableInfo.ColumnNames,
			colTypes:    tableInfo.ColumnTypes,
			keyCols:     tableInfo.PrimaryKeyCols,
			colsVisible: tableInfo.ColsVisible,
			rowsFactory: common.NewRowsFactory(tableInfo.ColumnTypes),
		},
		TableInfo:         tableInfo,
		store:             store,
		consumingNodes:    make(map[string]PushExecutor),
		delayer:           interruptor.GetInterruptManager(),
		transient:         transient,
		retentionDuration: retentionDuration,
		rowsFilledCounter: rowsFilledCounter,
	}
}

func (t *TableExecutor) IsTransient() bool {
	return t.transient
}

func (t *TableExecutor) ReCalcSchemaFromChildren() error {
	return nil
}

func (t *TableExecutor) AddConsumingNode(consumerName string, node PushExecutor) {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.addConsumingNode(consumerName, node)
}

func (t *TableExecutor) addConsumingNode(consumerName string, node PushExecutor) {
	t.consumingNodes[consumerName] = node
}

func (t *TableExecutor) RemoveConsumingNode(consumerName string) {
	t.lock.Lock()
	defer t.lock.Unlock()
	delete(t.consumingNodes, consumerName)
}

func (t *TableExecutor) HandleRemoteRows(rowsBatch RowsBatch, ctx *ExecutionContext) error {
	return t.HandleRows(rowsBatch, ctx)
}

func (t *TableExecutor) HandleRows(rowsBatch RowsBatch, ctx *ExecutionContext) error {
	t.lock.RLock()
	defer t.lock.RUnlock()

	if t.transient {
		return t.ForwardToConsumingNodes(rowsBatch, ctx)
	}

	rc := 0

	var outRows *common.Rows
	var entries []RowsEntry
	numEntries := rowsBatch.Len()
	hasDownStream := len(t.consumingNodes) > 0 || t.filling
	if hasDownStream {
		outRows = t.rowsFactory.NewRows(numEntries)
		entries = make([]RowsEntry, numEntries)
	}
	for i := 0; i < numEntries; i++ {
		prevRow := rowsBatch.PreviousRow(i)
		currentRow := rowsBatch.CurrentRow(i)

		if currentRow != nil {
			keyBuff := table.EncodeTableKeyPrefix(t.TableInfo.ID, ctx.WriteBatch.ShardID, 32)
			keyBuff, err := common.EncodeKeyCols(currentRow, t.TableInfo.PrimaryKeyCols, t.TableInfo.ColumnTypes, keyBuff)
			if err != nil {
				return errors.WithStack(err)
			}
			if hasDownStream {
				// We do a linearizable get as there is the possibility that the previous write for the same key has not
				// yet been applied to the state machine of the replica where the processor is running. Raft only
				// requires replication to a quorum for write to complete and that quorum might not contain the processor
				// replica
				v, err := ctx.Getter.Get(keyBuff)
				if err != nil {
					return errors.WithStack(err)
				}
				pi := -1
				if v != nil {
					// Row already exists in storage - this is the case where a new rows comes into a source for the same key
					// we won't have previousRow provided for us in this case as Kafka does not provide this
					if err := common.DecodeRow(v, t.colTypes, outRows); err != nil {
						return errors.WithStack(err)
					}
					pi = rc
					rc++
				}
				outRows.AppendRow(*currentRow)
				ci := rc
				rc++
				entries[i].prevIndex = pi
				entries[i].currIndex = ci
				entries[i].receiverIndex = rowsBatch.ReceiverIndex(i)
			}
			var valueBuff []byte
			valueBuff, err = common.EncodeRow(currentRow, t.colTypes, valueBuff)
			if err != nil {
				return errors.WithStack(err)
			}
			ctx.WriteBatch.AddPut(keyBuff, valueBuff)
		} else {
			// It's a delete
			keyBuff := table.EncodeTableKeyPrefix(t.TableInfo.ID, ctx.WriteBatch.ShardID, 32)
			keyBuff, err := common.EncodeKeyCols(prevRow, t.TableInfo.PrimaryKeyCols, t.colTypes, keyBuff)
			if err != nil {
				return errors.WithStack(err)
			}
			if hasDownStream {
				outRows.AppendRow(*prevRow)
				entries[i].prevIndex = rc
				entries[i].currIndex = -1
				entries[i].receiverIndex = rowsBatch.ReceiverIndex(i)
				rc++
			}
			ctx.WriteBatch.AddDelete(keyBuff)
		}
	}
	if hasDownStream {
		err := t.handleForwardAndCapture(NewRowsBatch(outRows, entries), ctx)
		return errors.WithStack(err)
	}
	return nil
}

func (t *TableExecutor) handleForwardAndCapture(rowsBatch RowsBatch, ctx *ExecutionContext) error {
	if err := t.ForwardToConsumingNodes(rowsBatch, ctx); err != nil {
		return errors.WithStack(err)
	}
	if t.filling && rowsBatch.Len() != 0 {
		if err := t.captureChanges(t.fillTableID, rowsBatch, ctx); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func (t *TableExecutor) ForwardToConsumingNodes(rowsBatch RowsBatch, ctx *ExecutionContext) error {
	for _, consumingNode := range t.consumingNodes {
		if err := consumingNode.HandleRows(rowsBatch, ctx); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func (t *TableExecutor) RowsFactory() *common.RowsFactory {
	return t.rowsFactory
}

func (t *TableExecutor) captureChanges(fillTableID uint64, rowsBatch RowsBatch, ctx *ExecutionContext) error {
	shardID := ctx.WriteBatch.ShardID
	ls, ok := t.lastSequences.Load(shardID)
	var nextSeq int64
	if !ok {
		nextSeq = 0
	} else {
		nextSeq = ls.(int64) + 1
	}
	wb := cluster.NewWriteBatch(shardID)
	numRows := rowsBatch.Len()
	for i := 0; i < numRows; i++ {
		row := rowsBatch.CurrentRow(i)
		key := table.EncodeTableKeyPrefix(fillTableID, shardID, 24)
		key = common.KeyEncodeInt64(key, nextSeq)
		value, err := common.EncodeRow(row, t.colTypes, nil)
		if err != nil {
			return errors.WithStack(err)
		}
		wb.AddPut(key, value)
		nextSeq++
	}
	t.lastSequences.Store(shardID, nextSeq-1)
	return t.store.WriteBatchLocally(wb)
}

func (t *TableExecutor) addFillTableToDelete(newTableID uint64, fillTableID uint64, schedulers map[uint64]*sched.ShardScheduler) (*cluster.ToDeleteBatch, error) {
	var prefixes [][]byte
	for shardID := range schedulers {
		prefix := table.EncodeTableKeyPrefix(fillTableID, shardID, 16)
		prefixes = append(prefixes, prefix)
	}
	batch := &cluster.ToDeleteBatch{
		ConditionalTableID: newTableID,
		Prefixes:           prefixes,
	}
	return batch, t.store.AddToDeleteBatch(batch)
}

func forAllSchedulers(schedulers map[uint64]*sched.ShardScheduler, f func(scheduler *sched.ShardScheduler)) {
	chans := make([]chan struct{}, len(schedulers))
	i := 0
	for _, scheduler := range schedulers {
		ch := make(chan struct{}, 1)
		chans[i] = ch
		i++
		theScheduler := scheduler
		go func() {
			f(theScheduler)
			ch <- struct{}{}
		}()
	}
	for _, ch := range chans {
		<-ch
	}
}

func (t *TableExecutor) pauseProcessing(schedulers map[uint64]*sched.ShardScheduler) {
	forAllSchedulers(schedulers, func(scheduler *sched.ShardScheduler) {
		scheduler.PauseProcessing()
	})
}

func (t *TableExecutor) resumeProcessing(schedulers map[uint64]*sched.ShardScheduler) {
	forAllSchedulers(schedulers, func(scheduler *sched.ShardScheduler) {
		scheduler.ResumeProcessing()
	})
}

// FillTo - fills the specified PushExecutor with all the rows in the table and also captures any new changes that
// might arrive while the fill is in progress. Once the fill is complete and the table executor and the push executor
// are in sync then the operation completes
//nolint:gocyclo
func (t *TableExecutor) FillTo(pe PushExecutor, consumerName string, newTableID uint64,
	schedulers map[uint64]*sched.ShardScheduler, failInject failinject.Injector, interruptor *interruptor.Interruptor) error {

	log.Debugf("table fill started for %d", newTableID)

	fillTableID, err := t.store.GenerateClusterSequence("table")
	if err != nil {
		return errors.WithStack(err)
	}
	fillTableID += common.UserTableIDBase

	toDeleteBatch, err := t.addFillTableToDelete(newTableID, fillTableID, schedulers)
	if err != nil {
		return err
	}

	// We pause processing to make sure there are no in process batches that have not yet committed yet
	t.pauseProcessing(schedulers)

	// Set filling to true, this will result in all incoming rows for the duration of the fill also being written to a
	// special table to capture them, we will need these once we have built the MV from the snapshot
	t.lock.Lock()
	t.filling = true
	t.fillTableID = fillTableID
	t.lock.Unlock()

	receiverSequences := sync.Map{}

	// start the fill - this takes a snapshot and fills from there
	ch, err := t.startReplayFromSnapshot(pe, schedulers, interruptor, &receiverSequences, fillTableID)
	if err != nil {
		t.resumeProcessing(schedulers)
		return errors.WithStack(err)
	}

	// We can now resume processing rows while we are filling
	t.resumeProcessing(schedulers)

	// We wait until the snapshot has been fully processed
	err, ok := <-ch
	if !ok {
		return errors.Error("channel was closed")
	}
	if err != nil {
		return errors.WithStack(err)
	}

	log.Debug("Snapshot processed - Now replaying any rows received in mean-time")

	// Now we need to replay the rows that were added from when we started the snapshot to now
	// we do this in batches
	startSeqs := make(map[uint64]int64)
	lockAndLoad := false
	for !lockAndLoad {

		if t.delayer.MaybeInterrupt("mv_replay", interruptor) {
			return errors.NewPranaErrorf(errors.DdlCancelled, "Loading initial state cancelled")
		}

		// Lock again and get the latest fill sequence
		t.pauseProcessing(schedulers)

		//startSequences is inclusive, endSequences is exclusive
		endSequences := make(map[uint64]int64)
		rowsToFill := 0
		// Compute the shards that need to be filled and the sequences

		t.lastSequences.Range(func(key, value interface{}) bool {
			k := key.(uint64)  //nolint:forcetypeassert
			v := value.(int64) //nolint:forcetypeassert
			prev, ok := startSeqs[k]
			if !ok {
				prev = -1
			}
			if v > prev {
				endSequences[k] = v + 1
				rowsToFill += int(v - prev)
				if prev == -1 {
					startSeqs[k] = 0
				}
			}
			return true
		})

		// If there's less than lockAndLoadMaxRows rows to catch up, we lock the whole source while we do it
		// Then we know we are up to date. The trick is to make sure this number is small so we don't lock
		// the executor for too long
		lockAndLoad = rowsToFill < lockAndLoadMaxRows

		log.Debugf("There are %d rows to replay", rowsToFill)

		if !lockAndLoad {
			// Too many rows to yet to replay so we resume processing while we replay what we've got
			t.resumeProcessing(schedulers)
		}

		// Now we replay those records to that sequence value
		if rowsToFill != 0 {
			if err := t.replayChanges(startSeqs, endSequences, pe, fillTableID, &receiverSequences, schedulers); err != nil {
				if lockAndLoad {
					t.resumeProcessing(schedulers)
				}
				return errors.WithStack(err)
			}
		}
		// Update the start sequences
		for k, v := range endSequences {
			startSeqs[k] = v
		}
	}

	// We're done, so we reset the state
	t.lock.Lock()
	t.filling = false
	// Reset the sequences
	t.lastSequences = sync.Map{}
	t.addConsumingNode(consumerName, pe)
	t.lock.Unlock()

	t.resumeProcessing(schedulers)

	// Delete the fill table data
	tableStartPrefix := common.AppendUint64ToBufferBE(nil, fillTableID)
	tableEndPrefix := common.AppendUint64ToBufferBE(nil, fillTableID+1)
	for shardID := range schedulers {
		if err := t.store.DeleteAllDataInRangeForShardLocally(shardID, tableStartPrefix, tableEndPrefix); err != nil {
			return errors.WithStack(err)
		}
	}

	// Maybe inject an error after fill but before the temp fill data has been deleted
	if err := failInject.GetFailpoint("fill_to_1").CheckFail(); err != nil {
		return err
	}

	if err := t.store.RemoveToDeleteBatch(toDeleteBatch); err != nil {
		return err
	}

	log.Debug("Table executor fill complete")

	return nil
}

func (t *TableExecutor) startReplayFromSnapshot(pe PushExecutor, schedulers map[uint64]*sched.ShardScheduler,
	interruptor *interruptor.Interruptor, receiverSeqs *sync.Map, fillTableID uint64) (chan error, error) {

	log.Debug("starting snapshot replay")
	snapshot, err := t.store.CreateSnapshot()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	ch := make(chan error, 1)
	go func() {
		err := t.performReplayFromSnapshot(snapshot, pe, schedulers, interruptor, receiverSeqs, fillTableID)
		snapshot.Close()
		ch <- err
	}()

	return ch, nil
}

func (t *TableExecutor) performReplayFromSnapshot(snapshot cluster.Snapshot, pe PushExecutor, schedulers map[uint64]*sched.ShardScheduler,
	interruptor *interruptor.Interruptor, receiverSeqs *sync.Map, fillTableID uint64) error {
	chans := make([]chan error, 0, len(schedulers))

	for shardID, scheduler := range schedulers {
		theScheduler := scheduler
		startPrefix := table.EncodeTableKeyPrefix(t.TableInfo.ID, shardID, 16)
		endPrefix := table.EncodeTableKeyPrefix(t.TableInfo.ID+1, shardID, 16)
		ch := make(chan error)
		// We execute the snapshot fill in parallel for each shard
		// And on each shard in batches of fillMaxBatchSize, executed on the processor
		// Doing them in batches allows the processor to handle process batches and forward writes so the fill
		// doesn't starve them out
		go func() {
			complete := &common.AtomicBool{}
			for !complete.Get() {
				action := func() error {
					if t.delayer.MaybeInterrupt("fill_snapshot", interruptor) {
						return errors.NewPranaErrorf(errors.DdlCancelled, "Loading initial state cancelled")
					}
					kvp, err := t.store.LocalScanWithSnapshot(snapshot, startPrefix, endPrefix, fillMaxBatchSize)
					if err != nil {
						return err
					}
					if len(kvp) != 0 {
						log.Debugf("shard %d replaying snapshot fill batch of of size %d", theScheduler.ShardID(), len(kvp))
						if err := t.sendFillBatchFromPairs(pe, theScheduler, kvp, receiverSeqs, fillTableID); err != nil {
							return err
						}
					}
					if len(kvp) < fillMaxBatchSize {
						// We're done for this shard
						complete.Set(true)
						return nil
					}
					startPrefix = common.IncrementBytesBigEndian(kvp[len(kvp)-1].Key)
					return nil
				}
				ach, err := theScheduler.AddAction(action, fillMaxBatchSize)
				if err != nil {
					ch <- err
					return
				}
				err = <-ach
				if err != nil {
					ch <- err
					return
				}
			}
			ch <- nil
		}()
		chans = append(chans, ch)
	}
	var err error
	for _, ch := range chans {
		var ok bool
		err, ok = <-ch
		if !ok {
			return errors.Error("channel was closed")
		}
		if err != nil {
			perr, ok := err.(errors.PranaError)
			if !ok || perr.Code != errors.DdlCancelled {
				// Return immediately
				return errors.WithStack(err)
			}
			// Otherwise we wait for all goroutines to cancel before returning
			log.Debug("Waiting for all snapshot fill goroutines to complete")
		}
	}
	return errors.WithStack(err)
}

// replay rows that were received while we were loading from the snapshot
func (t *TableExecutor) replayChanges(startSeqs map[uint64]int64, endSeqs map[uint64]int64, pe PushExecutor,
	fillTableID uint64, receiverSeqs *sync.Map, schedulers map[uint64]*sched.ShardScheduler) error {

	chans := make([]chan error, 0)
	for shardID, endSeq := range endSeqs {

		startSeq, ok := startSeqs[shardID]
		if !ok {
			panic("no start sequence")
		}

		theStartSeq := startSeq
		theEndSeq := endSeq
		if theEndSeq <= theStartSeq {
			panic("end seq <= start seq")
		}
		theSched, ok := schedulers[shardID]
		if !ok {
			panic("cannot find scheduler")
		}

		ch := make(chan error, 1)
		chans = append(chans, ch)

		// We do the replay in parallel for each shard, and we execute the fill for each shard on the processor for the
		// shard, repeating this if the amount of rows exceeds the batch size.
		// We execute on the processor so ingestion doesn't starve out the fill process
		go func() {
			var nextSeq atomic.Value
			nextSeq.Store(theStartSeq)
			action := func() error {
				log.Debugf("shard %d replaying captured fill batch of of size %d", theSched.ShardID(), theEndSeq-nextSeq.Load().(int64))
				lastSeq, err := t.scanAndSendFromFillTable(fillTableID, theSched, nextSeq.Load().(int64), theEndSeq, pe, receiverSeqs)
				if err != nil {
					return err
				}
				nextSeq.Store(lastSeq + 1)
				return nil
			}
			for nextSeq.Load().(int64) != theEndSeq {
				numRows := theEndSeq - nextSeq.Load().(int64)
				actCh, err := theSched.AddAction(action, int(numRows))
				if err != nil {
					ch <- err
					return
				}
				err = <-actCh
				if err != nil {
					ch <- err
					return
				}
			}
			ch <- nil
		}()
	}
	for _, ch := range chans {
		err, ok := <-ch
		if !ok {
			return errors.Error("channel was closed")
		}
		if err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func (t *TableExecutor) scanAndSendFromFillTable(fillTableID uint64, scheduler *sched.ShardScheduler, startSeq int64, endSeq int64,
	pe PushExecutor, receiverSeqs *sync.Map) (int64, error) {
	shardID := scheduler.ShardID()
	startPrefix := table.EncodeTableKeyPrefix(fillTableID, shardID, 24)
	startPrefix = common.KeyEncodeInt64(startPrefix, startSeq)
	endPrefix := table.EncodeTableKeyPrefix(fillTableID, shardID, 24)
	endPrefix = common.KeyEncodeInt64(endPrefix, endSeq)
	kvp, err := t.store.LocalScan(startPrefix, endPrefix, fillMaxBatchSize)
	if err != nil {
		return 0, err
	}
	log.Debugf("shard id %d sending fill batch from start %d to end %d length %d", shardID, startSeq, endSeq, len(kvp))
	err = t.sendFillBatchFromPairs(pe, scheduler, kvp, receiverSeqs, fillTableID)
	if err != nil {
		return 0, err
	}
	lastSeq, _ := common.KeyDecodeInt64(kvp[len(kvp)-1].Key, 16)
	return lastSeq, nil
}

func (t *TableExecutor) sendFillBatchFromPairs(pe PushExecutor, scheduler *sched.ShardScheduler, kvp []cluster.KVPair,
	receiverSeqs *sync.Map, fillTableID uint64) error {
	rows := t.RowsFactory().NewRows(len(kvp))
	for _, kv := range kvp {
		if err := common.DecodeRow(kv.Value, t.colTypes, rows); err != nil {
			return errors.WithStack(err)
		}
	}
	wb := cluster.NewWriteBatch(scheduler.ShardID())

	// When we fill it's important that we have duplicate detection as forward writes caused in fills (e.g. for aggregations)
	// can be retried in case of Raft timeout.
	// However we don't have a natural receiver sequence in this case. So we generate one starting from 1 at the beginning of the fill
	// We also will use the fill table id in the originator id when filling to distinguish the fill dup keys from normal dup keys
	var receiverSeq uint64
	lrs, ok := receiverSeqs.Load(scheduler.ShardID())
	if ok {
		receiverSeq, ok = lrs.(uint64)
		if !ok {
			panic("not a uint64")
		}
	}
	receiverSeq++ // We start at 1
	numRows := rows.RowCount()
	entries := make([]RowsEntry, numRows, numRows)
	for i := 0; i < numRows; i++ {
		entry := NewRowsEntry(-1, i, int64(receiverSeq))
		entries[i] = entry
		receiverSeq++
	}
	receiverSeqs.Store(scheduler.ShardID(), receiverSeq)
	batch := NewRowsBatch(rows, entries)

	ctx := NewExecutionContext(wb, scheduler, int64(fillTableID))
	if err := pe.HandleRows(batch, ctx); err != nil {
		return errors.WithStack(err)
	}
	if err := util.SendForwardBatches(ctx.RemoteBatches, t.store, true, true); err != nil {
		return err
	}
	if err := t.store.WriteBatch(wb, true); err != nil {
		return err
	}
	t.rowsFilledCounter.Add(float64(batch.Len()))
	return nil
}

func (t *TableExecutor) GetConsumerNames() []string {
	var mvNames []string
	for mvName := range t.consumingNodes {
		mvNames = append(mvNames, mvName)
	}
	return mvNames
}
