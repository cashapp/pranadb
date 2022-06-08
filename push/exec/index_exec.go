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
			keyBuff, valueBuff, err := t.createKeyBuff(ctx.WriteBatch.ShardID, currentRow)
			if err != nil {
				return errors.WithStack(err)
			}
			ctx.WriteBatch.AddPut(keyBuff, valueBuff)
		} else {
			// It's a delete
			keyBuff, _, err := t.createKeyBuff(ctx.WriteBatch.ShardID, prevRow)
			if err != nil {
				return errors.WithStack(err)
			}
			ctx.WriteBatch.AddDelete(keyBuff)
		}
	}
	return nil
}

func (t *IndexExecutor) createKeyBuff(shardID uint64, row *common.Row) ([]byte, []byte, error) {
	keyBuff := table.EncodeTableKeyPrefix(t.IndexInfo.ID, shardID, 32)
	keyBuff, err := common.EncodeIndexKeyCols(row, t.IndexInfo.IndexCols, t.TableInfo.ColumnTypes, keyBuff)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	pkStart := len(keyBuff)
	// We encode the PK cols on both the end of the key and the value
	// It needs to be on the key to make the entry unique (for non unique indexes)
	// and on the value so we can make looking up the PK easy for non covering indexes without having to parse the
	// whole key
	keyBuff, err = common.EncodeKeyCols(row, t.TableInfo.PrimaryKeyCols, t.TableInfo.ColumnTypes, keyBuff)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	valueBuff := keyBuff[pkStart:] // Value is just the PK
	return keyBuff, valueBuff, nil
}
