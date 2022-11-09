package exec

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/interruptor"
	"github.com/squareup/pranadb/table"
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
	ingestOnFirstMV   bool
	storeTombstones   bool
	retentionDuration time.Duration
	rowsFilledCounter prometheus.Counter
}

func NewTableExecutor(tableInfo *common.TableInfo, store cluster.Cluster, transient, ingestOnFirstMV bool,
	retentionDuration time.Duration, storeTombstones bool) *TableExecutor {
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
		ingestOnFirstMV:   ingestOnFirstMV,
		retentionDuration: retentionDuration,
		rowsFilledCounter: rowsFilledCounter,
		storeTombstones:   storeTombstones,
	}
}

func (t *TableExecutor) IsTransient() bool {
	return t.transient
}

func (t *TableExecutor) IngestOnFirstMV() bool {
	return t.ingestOnFirstMV
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
	// Some down stream consumers require the previous row
	requiresPreviousRow := len(t.consumingNodes) > 0 || t.filling
	if requiresPreviousRow {
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
			if requiresPreviousRow {
				v, err := ctx.RowCache.Get(keyBuff)
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
			if requiresPreviousRow {
				outRows.AppendRow(*prevRow)
				entries[i].prevIndex = rc
				entries[i].currIndex = -1
				entries[i].receiverIndex = rowsBatch.ReceiverIndex(i)
				rc++
			}
			if t.storeTombstones {
				// We don't delete the row, we store a tombstone - this is used in a sink where we need to keep track
				// of deleted rows too
				ctx.WriteBatch.AddPut(keyBuff, nil)
			} else {
				ctx.WriteBatch.AddDelete(keyBuff)
			}
		}
	}
	return t.handleForwardAndCapture(NewRowsBatch(outRows, entries), ctx)
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

func (t *TableExecutor) GetConsumerNames() []string {
	var mvNames []string
	for mvName := range t.consumingNodes {
		mvNames = append(mvNames, mvName)
	}
	return mvNames
}
