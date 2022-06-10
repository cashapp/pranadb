package exec

import (
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
		if currentRow != nil {
			keyBuff, valueBuff, err := table.EncodeIndexKeyValue(t.TableInfo, t.IndexInfo, ctx.WriteBatch.ShardID, currentRow)
			if err != nil {
				return errors.WithStack(err)
			}
			ctx.WriteBatch.AddPut(keyBuff, valueBuff)
		} else {
			// It's a delete
			keyBuff, _, err := table.EncodeIndexKeyValue(t.TableInfo, t.IndexInfo, ctx.WriteBatch.ShardID, prevRow)
			if err != nil {
				return errors.WithStack(err)
			}
			ctx.WriteBatch.AddDelete(keyBuff)
		}
	}
	return nil
}
