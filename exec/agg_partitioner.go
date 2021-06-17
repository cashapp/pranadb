package exec

import (
	"fmt"
	"github.com/squareup/pranadb/common"
)

type AggPartitioner struct {
	pushExecutorBase
	groupByExpressions []*common.Expression
	groupByCols        []int
	tableID            uint64
}

func NewAggPartitioner(colNames []string, colTypes []common.ColumnType, aggFunctions []common.AggFunction,
	groupByExpressions []*common.Expression, groupByCols []int, tableID uint64) (*AggPartitioner, error) {
	rf, err := common.NewRowsFactory(colTypes)
	if err != nil {
		return nil, err
	}
	base := pushExecutorBase{
		colNames:    colNames,
		colTypes:    colTypes,
		rowsFactory: rf,
	}
	return &AggPartitioner{
		pushExecutorBase:   base,
		groupByExpressions: groupByExpressions,
		groupByCols:        groupByCols,
		tableID:            tableID,
	}, nil
}

func (a *AggPartitioner) HandleRows(rows *common.PushRows, ctx *ExecutionContext) error {

	for i := 0; i < rows.RowCount(); i++ {

		row := rows.GetRow(i)

		// We evaluate the group by expressions to get the key

		key := make([]byte, 0, 8)

		for i, expr := range a.groupByExpressions {
			colType := a.colTypes[a.groupByCols[i]]
			switch colType {
			case common.TypeTinyInt, common.TypeInt, common.TypeBigInt:
				val, null, err := expr.EvalInt64(&row)
				if err != nil {
					return err
				}
				if null {
					key = append(key, 0)
				} else {
					key = common.EncodeInt64(val, key)
				}
			case common.TypeDecimal:
				_, null, err := expr.EvalDecimal(&row)
				if err != nil {
					return err
				}
				if null {
					key = append(key, 0)
				} else {
					panic("not implemented")
					// TODO
				}
			case common.TypeVarchar:
				val, null, err := expr.EvalString(&row)
				if err != nil {
					return err
				}
				if null {
					key = append(key, 0)
				} else {
					key = common.EncodeString(val, key)
				}
			case common.TypeDouble:
				val, null, err := expr.EvalFloat64(&row)
				if err != nil {
					return err
				}
				if null {
					key = append(key, 0)
				} else {
					key = common.EncodeFloat64(val, key)
				}
			default:
				return fmt.Errorf("unexpected column type %d", colType)
			}
		}

		// Send the row to the aggregator node which most likely lives in a different shard
		err := ctx.Forwarder.QueueForRemoteSend(key, &row, ctx.WriteBatch.ShardID, a.tableID, a.colTypes, ctx.WriteBatch)
		if err != nil {
			return err
		}
	}

	return nil
}

func (a *AggPartitioner) ReCalcSchemaFromChildren() {
	// NOOP
}
