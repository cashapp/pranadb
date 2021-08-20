package exec

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/push/mover"
	"github.com/squareup/pranadb/push/sched"
	"github.com/squareup/pranadb/table"
	"sync"
)

const lockAndLoadMaxRows = 10
const fillMaxBatchSize = 1000

// TableExecutor updates the changes into the associated table - used to persist state
// of a materialized view or source
type TableExecutor struct {
	pushExecutorBase
	tableInfo      *common.TableInfo
	consumingNodes map[PushExecutor]struct{}
	store          cluster.Cluster
	lock           sync.RWMutex
	filling        bool
	lastSequences  sync.Map
	fillTableID    uint64
}

func NewTableExecutor(colTypes []common.ColumnType, tableInfo *common.TableInfo, store cluster.Cluster) *TableExecutor {
	rf := common.NewRowsFactory(colTypes)
	pushBase := pushExecutorBase{
		colTypes:    colTypes,
		rowsFactory: rf,
	}
	return &TableExecutor{
		pushExecutorBase: pushBase,
		tableInfo:        tableInfo,
		store:            store,
		consumingNodes:   make(map[PushExecutor]struct{}),
	}
}

func (t *TableExecutor) ReCalcSchemaFromChildren() {
	if len(t.children) > 1 {
		panic("too many children")
	}
	if len(t.children) == 1 {
		child := t.children[0]
		t.colNames = child.ColNames()
		t.colTypes = child.ColTypes()
		t.keyCols = child.KeyCols()
	}
}

func (t *TableExecutor) AddConsumingNode(node PushExecutor) {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.addConsumingNode(node)
}

func (t *TableExecutor) addConsumingNode(node PushExecutor) {
	t.consumingNodes[node] = struct{}{}
}

func (t *TableExecutor) RemoveConsumingNode(node PushExecutor) {
	t.lock.Lock()
	defer t.lock.Unlock()
	delete(t.consumingNodes, node)
}

func (t *TableExecutor) HandleRemoteRows(rows *common.Rows, ctx *ExecutionContext) error {
	return t.HandleRows(rows, ctx)
}

func (t *TableExecutor) HandleRows(rows *common.Rows, ctx *ExecutionContext) error {
	t.lock.RLock()
	for i := 0; i < rows.RowCount(); i++ {
		row := rows.GetRow(i)
		if err := table.Upsert(t.tableInfo, &row, ctx.WriteBatch); err != nil {
			return err
		}
	}
	err := t.handleForwardAndCapture(rows, ctx)
	t.lock.RUnlock()
	return err
}

func (t *TableExecutor) handleForwardAndCapture(rows *common.Rows, ctx *ExecutionContext) error {
	if err := t.ForwardToConsumingNodes(rows, ctx); err != nil {
		return err
	}
	if t.filling && rows.RowCount() != 0 {
		if err := t.captureChanges(t.fillTableID, rows, ctx); err != nil {
			return err
		}
	}
	return nil
}

func (t *TableExecutor) ForwardToConsumingNodes(rows *common.Rows, ctx *ExecutionContext) error {
	for consumingNode := range t.consumingNodes {
		if err := consumingNode.HandleRows(rows, ctx); err != nil {
			return err
		}
	}
	return nil
}

func (t *TableExecutor) RowsFactory() *common.RowsFactory {
	return t.rowsFactory
}

func (t *TableExecutor) captureChanges(fillTableID uint64, rows *common.Rows, ctx *ExecutionContext) error {
	shardID := ctx.WriteBatch.ShardID
	ls, ok := t.lastSequences.Load(shardID)
	var nextSeq int64
	if !ok {
		nextSeq = 0
	} else {
		nextSeq = ls.(int64) + 1
	}
	wb := cluster.NewWriteBatch(shardID, false)
	for i := 0; i < rows.RowCount(); i++ {
		row := rows.GetRow(i)
		key := table.EncodeTableKeyPrefix(fillTableID, shardID, 24)
		key = common.KeyEncodeInt64(key, nextSeq)
		value, err := common.EncodeRow(&row, t.colTypes, nil)
		if err != nil {
			return err
		}
		wb.AddPut(key, value)
		nextSeq++
	}
	t.lastSequences.Store(shardID, nextSeq-1)
	return t.store.WriteBatch(wb)
}

func (t *TableExecutor) FillTo(pe PushExecutor, schedulers map[uint64]*sched.ShardScheduler, mover *mover.Mover, fillTableID uint64) error {

	// We need to pause all schedulers for the shards here.
	// We must make sure there are no in-progress and uncommitted upserts for the source as they won't appear in the
	// snapshot otherwise, and they won't be captured
	for _, sched := range schedulers {
		sched.Pause()
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
		return err
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
		return err
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
				return err
			}
		}
		// Update the start sequences
		for k, v := range endSequences {
			startSeqs[k] = v
		}
	}
	t.filling = false

	t.addConsumingNode(pe)

	t.lock.Unlock()

	// Delete the fill table data
	tableStartPrefix := common.AppendUint64ToBufferBE(nil, fillTableID)
	tableEndPrefix := common.AppendUint64ToBufferBE(nil, fillTableID+1)
	for shardID := range schedulers {
		if err := t.store.DeleteAllDataInRangeForShard(shardID, tableStartPrefix, tableEndPrefix); err != nil {
			return err
		}
	}
	return nil
}

func (t *TableExecutor) startFillFromSnapshot(pe PushExecutor, schedulers map[uint64]*sched.ShardScheduler, mover *mover.Mover) (chan error, error) {
	snapshot, err := t.store.CreateSnapshot()
	if err != nil {
		return nil, err
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
			startPrefix := table.EncodeTableKeyPrefix(t.tableInfo.ID, theShardID, 16)
			endPrefix := table.EncodeTableKeyPrefix(t.tableInfo.ID+1, theShardID, 16)
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
			return err
		}
	}
	return nil
}

func (t *TableExecutor) sendFillBatchFromPairs(pe PushExecutor, shardID uint64, kvp []cluster.KVPair, mover *mover.Mover) error {
	rows := t.RowsFactory().NewRows(len(kvp))
	for _, kv := range kvp {
		if err := common.DecodeRow(kv.Value, t.colTypes, rows); err != nil {
			return err
		}
	}
	wb := cluster.NewWriteBatch(shardID, false)
	ctx := &ExecutionContext{
		WriteBatch: wb,
		Mover:      mover,
	}
	if err := pe.HandleRows(rows, ctx); err != nil {
		return err
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

			kvp, err := t.store.LocalScan(startPrefix, endPrefix, 1000000)
			if err != nil {
				ch <- err
				return
			}
			if len(kvp) != int(numRows) {
				ch <- fmt.Errorf("node %d expected %d rows got %d start seq %d end seq %d startPrefix %s endPrefix %s",
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
			return err
		}
	}
	return nil
}
