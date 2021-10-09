package exec

import (
	"sync"

	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/push/mover"
	"github.com/squareup/pranadb/push/sched"
	"github.com/squareup/pranadb/table"
)

const lockAndLoadMaxRows = 10
const fillMaxBatchSize = 1000

// TableExecutor updates the changes into the associated table - used to persist state
// of a materialized view or source
type TableExecutor struct {
	pushExecutorBase
	TableInfo      *common.TableInfo
	consumingNodes map[string]PushExecutor
	store          cluster.Cluster
	lock           sync.RWMutex
	filling        bool
	lastSequences  sync.Map
	fillTableID    uint64
}

func NewTableExecutor(tableInfo *common.TableInfo, store cluster.Cluster) *TableExecutor {
	return &TableExecutor{
		pushExecutorBase: pushExecutorBase{
			colNames:    tableInfo.ColumnNames,
			colTypes:    tableInfo.ColumnTypes,
			keyCols:     tableInfo.PrimaryKeyCols,
			colsVisible: tableInfo.ColsVisible,
			rowsFactory: common.NewRowsFactory(tableInfo.ColumnTypes),
		},
		TableInfo:      tableInfo,
		store:          store,
		consumingNodes: make(map[string]PushExecutor),
	}
}

func (t *TableExecutor) ReCalcSchemaFromChildren() error {
	return nil
}

func (t *TableExecutor) AddConsumingNode(mvName string, node PushExecutor) {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.addConsumingNode(mvName, node)
}

func (t *TableExecutor) addConsumingNode(mvName string, node PushExecutor) {
	t.consumingNodes[mvName] = node
}

func (t *TableExecutor) RemoveConsumingNode(mvName string) {
	t.lock.Lock()
	defer t.lock.Unlock()
	delete(t.consumingNodes, mvName)
}

func (t *TableExecutor) HandleRemoteRows(rowsBatch RowsBatch, ctx *ExecutionContext) error {
	return t.HandleRows(rowsBatch, ctx)
}

func (t *TableExecutor) HandleRows(rowsBatch RowsBatch, ctx *ExecutionContext) error {
	t.lock.RLock()
	numEntries := rowsBatch.Len()
	outRows := t.rowsFactory.NewRows(numEntries)
	rc := 0
	entries := make([]RowsEntry, numEntries)
	for i := 0; i < numEntries; i++ {
		prevRow := rowsBatch.PreviousRow(i)
		currentRow := rowsBatch.CurrentRow(i)

		if currentRow != nil {
			keyBuff := table.EncodeTableKeyPrefix(t.TableInfo.ID, ctx.WriteBatch.ShardID, 32)
			keyBuff, err := common.EncodeKeyCols(currentRow, t.TableInfo.PrimaryKeyCols, t.TableInfo.ColumnTypes, keyBuff)
			if err != nil {
				return errors.WithStack(err)
			}
			v, err := t.store.LocalGet(keyBuff)
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
			outRows.AppendRow(*prevRow)
			entries[i].prevIndex = rc
			entries[i].currIndex = -1
			rc++
			ctx.WriteBatch.AddDelete(keyBuff)
		}
	}
	err := t.handleForwardAndCapture(NewRowsBatch(outRows, entries), ctx)
	t.lock.RUnlock()
	return errors.WithStack(err)
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
	wb := cluster.NewWriteBatch(shardID, false)
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
	return t.store.WriteBatch(wb)
}

func (t *TableExecutor) FillTo(pe PushExecutor, mvName string, schedulers map[uint64]*sched.ShardScheduler, mover *mover.Mover) error { //nolint:gocyclo

	fillTableID, err := t.store.GenerateClusterSequence("table")
	if err != nil {
		return errors.WithStack(err)
	}
	fillTableID += common.UserTableIDBase

	// We need to pause all schedulers for the shards here.
	// We must make sure there are no in-progress and uncommitted upserts for the source as they won't appear in the
	// snapshot otherwise, and they won't be captured
	for _, sh := range schedulers {
		sh.Pause()
	}

	// Lock the executor so no rows can be processed
	t.lock.Lock()

	// Set filling to true, this will result in all incoming rows for the duration of the fill also being written to a
	// special table to capture them, we will need these once we have built the MV from the snapshot
	t.filling = true
	t.fillTableID = fillTableID

	// start the fill - this takes a snapshot and fills from there
	ch, err := t.startFillFromSnapshot(pe, schedulers, mover)
	if err != nil {
		t.lock.Unlock()
		return errors.WithStack(err)
	}

	// We can now unlock the source and it can continue processing rows while we are filling
	t.lock.Unlock()

	for _, sched := range schedulers {
		sched.Resume()
	}

	// We wait until the snapshot has been fully processed
	err, ok := <-ch
	if !ok {
		return errors.New("channel was closed")
	}
	if err != nil {
		return errors.WithStack(err)
	}

	// Now we need to feed in the rows that were added to while we were processing the snapshot
	startSeqs := make(map[uint64]int64)
	lockAndLoad := false
	for !lockAndLoad {

		// Lock again and get the latest fill sequence
		t.lock.Lock()

		//startSequences is inclusive, endSequences is exclusive
		endSequences := make(map[uint64]int64)
		rowsToFill := 0
		// Compute the shards that need to be filled and the sequences

		t.lastSequences.Range(func(key, value interface{}) bool {
			k, ok := key.(uint64)
			if !ok {
				panic("not a uint64")
			}
			v, ok := value.(int64)
			if !ok {
				panic("not a int64")
			}
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

		if !lockAndLoad {
			// Too many rows to lock while we fill, so we do this outside the lock and will go around the loop
			// again
			t.lock.Unlock()
		}

		// Now we replay those records to that sequence value
		if rowsToFill != 0 {
			if err := t.replayChanges(startSeqs, endSequences, pe, fillTableID, mover); err != nil {
				if lockAndLoad {
					t.lock.Unlock()
				}
				return errors.WithStack(err)
			}
		}
		// Update the start sequences
		for k, v := range endSequences {
			startSeqs[k] = v
		}
	}
	t.filling = false

	t.addConsumingNode(mvName, pe)

	t.lock.Unlock()

	// Delete the fill table data
	tableStartPrefix := common.AppendUint64ToBufferBE(nil, fillTableID)
	tableEndPrefix := common.AppendUint64ToBufferBE(nil, fillTableID+1)
	for shardID := range schedulers {
		if err := t.store.DeleteAllDataInRangeForShard(shardID, tableStartPrefix, tableEndPrefix); err != nil {
			return errors.WithStack(err)
		}
	}

	// The fill may cause forwarding of rows in case of an aggregation - so we need to trigger any transfer of data too
	for shardID, sched := range schedulers {
		sid := shardID
		sched.ScheduleActionFireAndForget(func() error {
			return mover.TransferData(sid, true)
		})
	}

	return nil
}

func (t *TableExecutor) startFillFromSnapshot(pe PushExecutor, schedulers map[uint64]*sched.ShardScheduler, mover *mover.Mover) (chan error, error) {
	snapshot, err := t.store.CreateSnapshot()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	ch := make(chan error, 1)
	go func() {
		err := t.performFillFromSnapshot(snapshot, pe, schedulers, mover)
		snapshot.Close()
		ch <- err
	}()
	return ch, nil
}

func (t *TableExecutor) performFillFromSnapshot(snapshot cluster.Snapshot, pe PushExecutor, schedulers map[uint64]*sched.ShardScheduler,
	mover *mover.Mover) error {
	numRows := 0
	chans := make([]chan error, 0, len(schedulers))

	for shardID := range schedulers {
		ch := make(chan error, 1)
		chans = append(chans, ch)
		// We don't execute on the shard schedulers - we can't as this would require schedulers to be running which would
		// mean remote batches could be handled and end up blocking on the source table executor mutex which would mean a batch waiting
		// behind it wouldn't get processed.
		// We run this with schedulers running - this is ok, as the MV dag is isolated at this point so there could be no
		// concurrent execution from schedulers
		theShardID := shardID
		go func() {
			startPrefix := table.EncodeTableKeyPrefix(t.TableInfo.ID, theShardID, 16)
			endPrefix := table.EncodeTableKeyPrefix(t.TableInfo.ID+1, theShardID, 16)
			for {
				kvp, err := t.store.LocalScanWithSnapshot(snapshot, startPrefix, endPrefix, fillMaxBatchSize)
				if err != nil {
					ch <- err
					return
				}
				if len(kvp) == 0 {
					ch <- nil
					return
				}
				if err := t.sendFillBatchFromPairs(pe, theShardID, kvp, mover); err != nil {
					ch <- err
					return
				}
				if len(kvp) < fillMaxBatchSize {
					// We're done for this shard
					ch <- nil
					return
				}
				startPrefix = common.IncrementBytesBigEndian(kvp[len(kvp)-1].Key)
				numRows += len(kvp)
			}
		}()
	}
	for _, ch := range chans {
		err, ok := <-ch
		if !ok {
			return errors.New("channel was closed")
		}
		if err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func (t *TableExecutor) sendFillBatchFromPairs(pe PushExecutor, shardID uint64, kvp []cluster.KVPair, mover *mover.Mover) error {
	rows := t.RowsFactory().NewRows(len(kvp))
	for _, kv := range kvp {
		if err := common.DecodeRow(kv.Value, t.colTypes, rows); err != nil {
			return errors.WithStack(err)
		}
	}
	wb := cluster.NewWriteBatch(shardID, false)
	ctx := &ExecutionContext{
		WriteBatch: wb,
		Mover:      mover,
	}
	if err := pe.HandleRows(NewCurrentRowsBatch(rows), ctx); err != nil {
		return errors.WithStack(err)
	}
	return t.store.WriteBatch(wb)
}

func (t *TableExecutor) replayChanges(startSeqs map[uint64]int64, endSeqs map[uint64]int64, pe PushExecutor,
	fillTableID uint64, mover *mover.Mover) error {

	chans := make([]chan error, 0)
	for shardID, endSeq := range endSeqs {

		ch := make(chan error, 1)
		chans = append(chans, ch)

		startSeq, ok := startSeqs[shardID]
		if !ok {
			panic("no start sequence")
		}

		theShardID := shardID
		theEndSeq := endSeq
		go func() {
			startPrefix := table.EncodeTableKeyPrefix(fillTableID, theShardID, 24)
			startPrefix = common.KeyEncodeInt64(startPrefix, startSeq)
			endPrefix := table.EncodeTableKeyPrefix(fillTableID, theShardID, 24)
			endPrefix = common.KeyEncodeInt64(endPrefix, theEndSeq)

			numRows := theEndSeq - startSeq

			if numRows == 0 {
				panic("num rows zero")
			}

			kvp, err := t.store.LocalScan(startPrefix, endPrefix, 1000000) // TODO don't hardcode here
			if err != nil {
				ch <- err
				return
			}
			if len(kvp) != int(numRows) {
				ch <- errors.Errorf("node %d expected %d rows got %d start seq %d end seq %d startPrefix %s endPrefix %s",
					t.store.GetNodeID(),
					numRows, len(kvp), startSeq, theEndSeq, common.DumpDataKey(startPrefix), common.DumpDataKey(endPrefix))
				return
			}
			if err := t.sendFillBatchFromPairs(pe, theShardID, kvp, mover); err != nil {
				ch <- err
				return
			}
			ch <- nil
		}()
	}
	for _, ch := range chans {
		err, ok := <-ch
		if !ok {
			return errors.New("channel was closed")
		}
		if err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func (t *TableExecutor) GetConsumingMvNames() []string {
	var mvNames []string
	for mvName := range t.consumingNodes {
		mvNames = append(mvNames, mvName)
	}
	return mvNames
}
