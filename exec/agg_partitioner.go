package exec

import (
	"github.com/squareup/pranadb/common"
)

type AggPartitioner struct {
	pushExecutorBase
	tableID uint64
}

func NewAggPartitioner(colNames []string, colTypes []common.ColumnType, keyCols []int, tableID uint64) (*AggPartitioner, error) {
	rf, err := common.NewRowsFactory(colTypes)
	if err != nil {
		return nil, err
	}
	base := pushExecutorBase{
		colNames:    colNames,
		colTypes:    colTypes,
		keyCols:     keyCols,
		rowsFactory: rf,
	}
	return &AggPartitioner{
		pushExecutorBase: base,
		tableID:          tableID,
	}, nil
}

func (a *AggPartitioner) HandleRows(rows *common.PushRows, ctx *ExecutionContext) error {

	for i := 0; i < rows.RowCount(); i++ {

		row := rows.GetRow(i)
		key := make([]byte, 0, 8)
		key, err := common.EncodeCols(&row, a.keyCols, a.colTypes, key)
		if err != nil {
			return err
		}

		// Send the row to the aggregator node which most likely lives in a different shard
		err = ctx.Forwarder.QueueForRemoteSend(key, &row, ctx.WriteBatch.ShardID, a.tableID, a.colTypes, ctx.WriteBatch)
		if err != nil {
			return err
		}
	}

	return nil
}

func (a *AggPartitioner) ReCalcSchemaFromChildren() {
	// NOOP
}
