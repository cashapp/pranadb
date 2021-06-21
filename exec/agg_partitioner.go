package exec

import (
	"github.com/squareup/pranadb/common"
	"log"
)

type AggPartitioner struct {
	pushExecutorBase
	tableID     uint64
	groupByCols []int
	sharder     common.Sharder
}

// TODO combine this with aggregator - no need to have two executors
func NewAggPartitioner(colNames []string, colTypes []common.ColumnType, keyCols []int, tableID uint64,
	groupByCols []int, sharder common.Sharder) (*AggPartitioner, error) {
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
		groupByCols:      groupByCols,
		sharder:          sharder,
	}, nil
}

func (a *AggPartitioner) HandleRows(rows *common.PushRows, ctx *ExecutionContext) error {

	for i := 0; i < rows.RowCount(); i++ {

		row := rows.GetRow(i)
		key := make([]byte, 0, 8)
		key, err := common.EncodeCols(&row, a.groupByCols, a.GetChildren()[0].ColTypes(), key)
		if err != nil {
			return err
		}

		log.Println("Agg partitioner forwarding row(s)")

		remoteShardID, err := a.sharder.CalculateShard(key)
		if remoteShardID == ctx.WriteBatch.ShardID {
			// Destination shard is same as this one, so just pass rows through to next
			// executor
			err := a.parent.HandleRows(rows, ctx)
			if err != nil {
				return err
			}
		} else {
			err = ctx.Forwarder.QueueForRemoteSend(key, remoteShardID, &row, ctx.WriteBatch.ShardID, a.tableID, a.colTypes, ctx.WriteBatch)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// ReCalcSchemaFromChildren Agg partitioner just forwards so it's schema is of its child not the aggregation
func (a *AggPartitioner) ReCalcSchemaFromChildren() {
	if len(a.children) > 1 {
		panic("too many children")
	}
	if len(a.children) == 1 {
		child := a.children[0]
		a.colNames = child.ColNames()
		a.colTypes = child.ColTypes()
		a.keyCols = child.KeyCols()
	}
}
