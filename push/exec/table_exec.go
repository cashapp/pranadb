package exec

import (
	"github.com/pkg/errors"
	"github.com/squareup/pranadb/push/mover"
	"sync"

	"github.com/squareup/pranadb/cluster"

	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/table"
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
	t.consumingNodes[node] = struct{}{}
}

func (t *TableExecutor) RemoveConsumingNode(node PushExecutor) {
	t.lock.Lock()
	defer t.lock.Unlock()
	delete(t.consumingNodes, node)
}

func (t *TableExecutor) HandleRemoteRows(rows *common.Rows, ctx *ExecutionContext) error {
	return t.handleRowsWithLock(rows, ctx)
}

func (t *TableExecutor) HandleRows(rows *common.Rows, ctx *ExecutionContext) error {
	return t.handleRowsWithLock(rows, ctx)
}

func (t *TableExecutor) handleRowsWithLock(rows *common.Rows, ctx *ExecutionContext) error {
	// Explicit locking is faster than using defer
	t.lock.RLock()
	err := t.handleRows(rows, ctx)
	t.lock.RUnlock()
	return err
}

func (t *TableExecutor) handleRows(rows *common.Rows, ctx *ExecutionContext) error {
	for i := 0; i < rows.RowCount(); i++ {
		row := rows.GetRow(i)
		err := table.Upsert(t.tableInfo, &row, ctx.WriteBatch)
		if err != nil {
			return err
		}
	}
	if t.filling {
		if err := t.captureChanges(t.fillTableID, rows, ctx); err != nil {
			return err
		}
	}

	return t.ForwardToConsumingNodes(rows, ctx)
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

func (t *TableExecutor) ForwardToConsumingNodes(rows *common.Rows, ctx *ExecutionContext) error {
	t.lock.Lock()
	defer t.lock.Unlock()
	for consumingNode := range t.consumingNodes {
		err := consumingNode.HandleRows(rows, ctx)
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *TableExecutor) RowsFactory() *common.RowsFactory {
	return t.rowsFactory
}

// FillTo Note this must not be called from the scheduler goroutine for the shard!
func (t *TableExecutor) FillTo(pe PushExecutor, shardIDs []uint64, mover *mover.Mover, fillTableID uint64) error {
	// Lock the executor so now rows can be processed
	t.lock.Lock()

	// Set filling to true, this will result in all incoming rows for the duration of the fill also being written to a
	// special table to capture them, we will need these once we have built the MV from the snapshot
	t.filling = true
	t.fillTableID = fillTableID

	// Start the fill - this takes a snapshot and fills from there
	ch, err := t.startFillFromSnapshot(pe, shardIDs, mover)
	if err != nil {
		t.lock.Unlock()
		return err
	}

	// We can now unlock the source and it can continue processing rows while we are filling
	t.lock.Unlock()

	// We wait until the snapshot has been fully processed
	_, ok := <-ch
	if !ok {
		return errors.New("channel was closed")
	}

	// Now we need to feed in the rows that were added to while we were processing the snapshot
	startSeqs := make(map[uint64]int64)
	lockAndLoad := false
	for !lockAndLoad {

		// Lock again and get the latest fill sequence
		t.lock.Lock()
		// Copy the map
		latestSeqs := make(map[uint64]int64)
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
			if v-prev > 0 {
				latestSeqs[k] = v
				rowsToFill += int(v - prev)
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
		if err := t.replayChanges(startSeqs, latestSeqs, pe, fillTableID, mover); err != nil {
			if lockAndLoad {
				t.lock.Unlock()
			}
			return err
		}
		for k, v := range latestSeqs {
			latestSeqs[k] = v + 1
		}
		startSeqs = latestSeqs
	}
	t.filling = false

	t.AddConsumingNode(pe)

	t.lock.Unlock()

	// Delete all the data for the fill table
	tableStartPrefix := common.AppendUint64ToBufferBE(nil, fillTableID)
	tableEndPrefix := common.AppendUint64ToBufferBE(nil, fillTableID+1)
	return t.store.DeleteAllDataInRange(tableStartPrefix, tableEndPrefix)
}

func (t *TableExecutor) startFillFromSnapshot(pe PushExecutor, shardIDs []uint64, mover *mover.Mover) (chan error, error) {
	snapshot, err := t.store.CreateSnapshot()
	if err != nil {
		return nil, err
	}
	ch := make(chan error, 1)
	go func() {
		err := t.performFillFromSnapshot(snapshot, pe, shardIDs, mover)
		snapshot.Close()
		ch <- err
	}()
	return ch, nil
}

func (t *TableExecutor) performFillFromSnapshot(snapshot cluster.Snapshot, pe PushExecutor, shardIDs []uint64, mover *mover.Mover) error {
outer:
	for _, shardID := range shardIDs {
		startPrefix := table.EncodeTableKeyPrefix(t.tableInfo.ID, shardID, 16)
		endPrefix := table.EncodeTableKeyPrefix(t.tableInfo.ID+1, shardID, 16)
		for {
			kvp, err := t.store.LocalScanWithSnapshot(snapshot, startPrefix, endPrefix, fillMaxBatchSize)
			if err != nil {
				return err
			}
			if len(kvp) == 0 {
				continue outer
			}
			if err := t.sendFillBatchFromPairs(pe, shardID, kvp, mover); err != nil {
				return err
			}
			if len(kvp) < fillMaxBatchSize {
				// We're done for this shard
				continue outer
			}
			startPrefix = common.IncrementBytesBigEndian(kvp[len(kvp)-1].Key)
		}
	}
	return nil
}

func (t *TableExecutor) sendFillBatchFromPairs(pe PushExecutor, shardID uint64, kvp []cluster.KVPair, mover *mover.Mover) error {

	// TODO needs to be executed on shard scheduler

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

func (t *TableExecutor) replayChanges(startSeqs map[uint64]int64, endSeqs map[uint64]int64, pe PushExecutor, fillTableID uint64, mover *mover.Mover) error {
	for shardID, endSeq := range endSeqs {

		// TODO Execute the body in parallel on shard scheduler

		startSeq, ok := startSeqs[shardID]
		if !ok {
			panic("no start sequence")
		}
		startPrefix := table.EncodeTableKeyPrefix(fillTableID, shardID, 24)
		startPrefix = common.KeyEncodeInt64(startPrefix, startSeq)
		endPrefix := table.EncodeTableKeyPrefix(fillTableID, shardID, 24)
		endPrefix = common.KeyEncodeInt64(endPrefix, endSeq)

		kvp, err := t.store.LocalScan(startPrefix, endPrefix, int(endSeq-startSeq+1))
		if err != nil {
			return err
		}
		if err := t.sendFillBatchFromPairs(pe, shardID, kvp, mover); err != nil {
			return err
		}
	}
	return nil
}
