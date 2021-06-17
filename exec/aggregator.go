package exec

import (
	"github.com/squareup/pranadb/common"
)

type Aggregator struct {
	pushExecutorBase
	aggFunctions       []common.AggFunction
	groupByExpressions []*common.Expression
	groupByCols        []int
	TableID            uint64
}

func NewAggregator(colNames []string, colTypes []common.ColumnType, aggFunctions []common.AggFunction,
	groupByExpressions []*common.Expression, groupByCols []int, tableID uint64) (*Aggregator, error) {
	rf, err := common.NewRowsFactory(colTypes)
	if err != nil {
		return nil, err
	}
	base := pushExecutorBase{
		colNames:    colNames,
		colTypes:    colTypes,
		rowsFactory: rf,
	}
	return &Aggregator{
		pushExecutorBase:   base,
		aggFunctions:       aggFunctions,
		groupByExpressions: groupByExpressions,
		groupByCols:        groupByCols,
		TableID:            tableID,
	}, nil
}

func (a *Aggregator) HandleRows(rows *common.PushRows, ctx *ExecutionContext) error {

	//for i := 0; i < rows.RowCount(); i++ {
	//
	//	row := rows.GetRow(i)
	//	println(row)
	//
	//}

	return nil
}

func (a *Aggregator) ReCalcSchemaFromChildren() {
	// NOOP
}
