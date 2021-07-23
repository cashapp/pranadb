package exec

import (
	"fmt"
	"log"

	"github.com/squareup/pranadb/aggfuncs"
	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/sharder"
	"github.com/squareup/pranadb/table"
)

type Aggregator struct {
	pushExecutorBase
	aggFuncs     []aggfuncs.AggregateFunction
	AggTableInfo *common.TableInfo
	groupByCols  []int
	storage      cluster.Cluster
	sharder      *sharder.Sharder
}

type AggregateFunctionInfo struct {
	FuncType aggfuncs.AggFunctionType
	Distinct bool
	ArgExpr  *common.Expression
}

func NewAggregator(colNames []string, colTypes []common.ColumnType, pkCols []int, aggFunctions []*AggregateFunctionInfo, aggTableInfo *common.TableInfo, groupByCols []int,
	storage cluster.Cluster, sharder *sharder.Sharder) (*Aggregator, error) {
	rf := common.NewRowsFactory(colTypes)
	pushBase := pushExecutorBase{
		colNames:    colNames,
		colTypes:    colTypes,
		keyCols:     pkCols,
		rowsFactory: rf,
	}
	aggFuncs, err := createAggFunctions(aggFunctions, colTypes)
	if err != nil {
		return nil, err
	}
	return &Aggregator{
		pushExecutorBase: pushBase,
		aggFuncs:         aggFuncs,
		AggTableInfo:     aggTableInfo,
		groupByCols:      groupByCols,
		storage:          storage,
		sharder:          sharder,
	}, nil
}

func createAggFunctions(aggFunctionInfos []*AggregateFunctionInfo, colTypes []common.ColumnType) ([]aggfuncs.AggregateFunction, error) {
	aggFuncs := make([]aggfuncs.AggregateFunction, len(aggFunctionInfos))
	for index, funcInfo := range aggFunctionInfos {
		argExpr := funcInfo.ArgExpr
		valueType := colTypes[index]
		aggFunc, err := aggfuncs.NewAggregateFunction(argExpr, funcInfo.FuncType, valueType)
		if err != nil {
			return nil, err
		}
		aggFuncs[index] = aggFunc
	}
	return aggFuncs, nil
}

func (a *Aggregator) HandleRows(rows *common.Rows, ctx *ExecutionContext) error {
	for i := 0; i < rows.RowCount(); i++ {

		row := rows.GetRow(i)
		key := make([]byte, 0, 8)
		incomingColTypes := a.children[0].ColTypes()
		key, err := common.EncodeCols(&row, a.groupByCols, incomingColTypes, key, false)
		if err != nil {
			return err
		}

		log.Println("Agg partitioner forwarding row(s)")

		remoteShardID, err := a.sharder.CalculateShard(sharder.ShardTypeHash, key)
		if err != nil {
			return err
		}
		if remoteShardID == ctx.WriteBatch.ShardID {
			// Destination shard is same as this one, so no need for remote send
			return a.HandleRemoteRows(rows, ctx)
		}
		err = ctx.Forwarder.QueueForRemoteSend(key, remoteShardID, &row, ctx.WriteBatch.ShardID, a.AggTableInfo.ID, incomingColTypes, ctx.WriteBatch)
		if err != nil {
			return err
		}
	}

	return nil
}

func (a *Aggregator) HandleRemoteRows(rows *common.Rows, ctx *ExecutionContext) error {

	aggStates := make(map[string]*aggfuncs.AggState)

	for i := 0; i < rows.RowCount(); i++ {
		row := rows.GetRow(i)
		err := a.calcAggregations(&row, ctx, aggStates)
		if err != nil {
			return err
		}
	}

	resultRows := a.rowsFactory.NewRows(len(aggStates))
	for _, aggState := range aggStates {
		for i, colType := range a.colTypes {
			if aggState.IsNull(i) {
				resultRows.AppendNullToColumn(i)
			} else {
				switch colType.Type {
				case common.TypeTinyInt, common.TypeInt, common.TypeBigInt:
					resultRows.AppendInt64ToColumn(i, aggState.GetInt64(i))
				case common.TypeDecimal:
					// TODO
				case common.TypeDouble:
					resultRows.AppendFloat64ToColumn(i, aggState.GetFloat64(i))
				case common.TypeVarchar:
					strPtr := aggState.GetString(i)
					resultRows.AppendStringToColumn(i, *strPtr)
				default:
					return fmt.Errorf("unexpected column type %d", colType)
				}
			}
		}
	}

	log.Printf("Aggregator writing %d rows into agg state", resultRows.RowCount())
	for i := 0; i < resultRows.RowCount(); i++ {
		row := resultRows.GetRow(i)
		err := table.Upsert(a.AggTableInfo, &row, ctx.WriteBatch)
		if err != nil {
			return err
		}
	}

	// FIXME - we don't output all result rows, only ones that have changed
	return a.parent.HandleRows(resultRows, ctx)
}

// The cols for the output of an aggregation are:
// one col holding each aggregate result in the order they appear in the select
// one col for each original column from the input in the same order as the input
// There is one agg function for each of the columns above, for aggregate columns,
// it's the actual aggregate function, otherwise it's a "firstrow" function
// TODO: break into smaller functions
// nolint: gocyclo
func (a *Aggregator) calcAggregations(row *common.Row, ctx *ExecutionContext, aggStates map[string]*aggfuncs.AggState) error {

	// TODO this seems unnecessary - we should just lookup the row in storage using the
	// keyBytes
	groupByVals := make([]interface{}, len(a.keyCols))
	for i, groupByCol := range a.groupByCols {
		colType := a.GetChildren()[0].ColTypes()[groupByCol]
		var val interface{}
		null := row.IsNull(groupByCol)
		if !null {
			switch colType.Type {
			case common.TypeTinyInt, common.TypeInt, common.TypeBigInt:
				val = row.GetInt64(groupByCol)
			case common.TypeDecimal:
				// TODO
			case common.TypeDouble:
				val = row.GetFloat64(groupByCol)
			case common.TypeVarchar:
				val = row.GetString(groupByCol)
			default:
				return fmt.Errorf("unexpected column type %d", colType)
			}
			groupByVals[i] = val
		}
	}

	keyBytes := make([]byte, 0, 8)
	keyBytes, err := common.EncodeCols(row, a.groupByCols, a.GetChildren()[0].ColTypes(), keyBytes, false)
	if err != nil {
		return err
	}

	aggState, ok := aggStates[common.ByteSliceToStringZeroCopy(keyBytes)]
	if !ok {

		// TODO Seems inefficient to have to encode the key again just to lookup the row -
		// we should lookup direct in storage - not use table
		currRow, err := table.LookupInPk(a.AggTableInfo, groupByVals, a.AggTableInfo.PrimaryKeyCols, ctx.WriteBatch.ShardID, a.rowsFactory, a.storage)

		if err != nil {
			return err
		}
		numCols := len(a.colTypes)
		aggState = aggfuncs.NewAggState(numCols)
		aggStates[common.ByteSliceToStringZeroCopy(keyBytes)] = aggState
		if currRow != nil {
			for i := 0; i < numCols; i++ {
				colType := a.colTypes[i]
				if currRow.IsNull(i) {
					aggState.SetNull(i, true)
				} else {
					switch colType.Type {
					case common.TypeTinyInt, common.TypeInt, common.TypeBigInt:
						aggState.SetInt64(i, currRow.GetInt64(i))
					case common.TypeDecimal:
						// TODO
					case common.TypeDouble:
						aggState.SetFloat64(i, currRow.GetFloat64(i))
					case common.TypeVarchar:
						strVal := currRow.GetString(i)
						aggState.SetString(i, &strVal)
					default:
						return fmt.Errorf("unexpected column type %d", colType)
					}
				}
			}
		}
	}

	for index, aggFunc := range a.aggFuncs {
		switch aggFunc.ValueType().Type {
		case common.TypeTinyInt, common.TypeInt, common.TypeBigInt:
			arg, null, err := aggFunc.ArgExpression().EvalInt64(row)
			if err != nil {
				return err
			}
			err = aggFunc.EvalInt64(arg, null, aggState, index)
			if err != nil {
				return err
			}
		case common.TypeDecimal:
			// TODO
		case common.TypeDouble:
			arg, null, err := aggFunc.ArgExpression().EvalFloat64(row)
			if err != nil {
				return err
			}
			err = aggFunc.EvalFloat64(arg, null, aggState, index)
			if err != nil {
				return err
			}
		case common.TypeVarchar:
			arg, null, err := aggFunc.ArgExpression().EvalString(row)
			if err != nil {
				return err
			}
			err = aggFunc.EvalString(arg, null, aggState, index)
			if err != nil {
				return err
			}
		default:
			return fmt.Errorf("unexpected column type %d", aggFunc.ValueType())
		}
	}

	return nil
}

func (a *Aggregator) ReCalcSchemaFromChildren() {
	// NOOP
}
