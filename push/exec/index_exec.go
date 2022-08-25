package exec

import (
	"bytes"
	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/table"
)

// IndexExecutor maintains a secondary index
type IndexExecutor struct {
	pushExecutorBase
	IndexInfo *common.IndexInfo
	TableInfo *common.TableInfo // The table info of the table (source or MV) that we are creating the index on
	store     cluster.Cluster
}

func NewIndexExecutor(tableInfo *common.TableInfo, indexInfo *common.IndexInfo, store cluster.Cluster) *IndexExecutor {
	return &IndexExecutor{
		pushExecutorBase: pushExecutorBase{
			rowsFactory: common.NewRowsFactory(tableInfo.ColumnTypes),
		},
		TableInfo: tableInfo,
		IndexInfo: indexInfo,
		store:     store,
	}
}

func (t *IndexExecutor) ReCalcSchemaFromChildren() error {
	return nil
}

func (t *IndexExecutor) HandleRows(rowsBatch RowsBatch, ctx *ExecutionContext) error {
	numEntries := rowsBatch.Len()
	for i := 0; i < numEntries; i++ {
		prevRow := rowsBatch.PreviousRow(i)
		currentRow := rowsBatch.CurrentRow(i)
		var err error
		var prevKey []byte
		if prevRow != nil {
			prevKey, _, err = table.EncodeIndexKeyValue(t.TableInfo, t.IndexInfo, ctx.WriteBatch.ShardID, prevRow)
			if err != nil {
				return errors.WithStack(err)
			}
		}
		var currKey []byte
		var currValue []byte
		if currentRow != nil {
			currKey, currValue, err = table.EncodeIndexKeyValue(t.TableInfo, t.IndexInfo, ctx.WriteBatch.ShardID, currentRow)
			if err != nil {
				return errors.WithStack(err)
			}
		}
		if bytes.Compare(prevKey, currKey) == 0 {
			// If the key hasn't changed so do nothing, this is important in the case of a last update index as two updates
			// can occur in same ms so key doesn't change, if we don't do nothing then we add a put and a delete for the
			// same key. But deletes always get processed in the state machine after puts for a batch which will result
			// in the index entry getting deleted
			continue
		}

		if prevRow != nil {
			// Delete any old entry
			ctx.WriteBatch.AddDelete(prevKey)
		}
		if currentRow != nil {
			// Put any new entry
			ctx.WriteBatch.AddPut(currKey, currValue)
		}
	}
	return nil
}
