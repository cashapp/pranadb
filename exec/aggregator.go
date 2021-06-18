package exec

import (
	"fmt"
	"github.com/squareup/pranadb/aggfuncs"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/table"
)

type Aggregator struct {
	pushExecutorBase
	aggFuncs []aggfuncs.AggregateFunction
	Table    table.Table
}

type AggregateFunctionInfo struct {
	FuncType aggfuncs.AggFunctionType
	Distinct bool
	ArgExpr  *common.Expression
}

func NewAggregator(colNames []string, colTypes []common.ColumnType, pkCols []int, aggFunctions []*AggregateFunctionInfo, aggTable table.Table) (*Aggregator, error) {
	rf, err := common.NewRowsFactory(colTypes)
	if err != nil {
		return nil, err
	}
	base := pushExecutorBase{
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
		pushExecutorBase: base,
		aggFuncs:         aggFuncs,
		Table:            aggTable,
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

func (a *Aggregator) HandleRows(rows *common.PushRows, ctx *ExecutionContext) error {

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
				switch colType {
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

	for i := 0; i < resultRows.RowCount(); i++ {
		row := resultRows.GetRow(i)
		err := a.Table.Upsert(&row, ctx.WriteBatch)
		if err != nil {
			return err
		}
	}

	return a.parent.HandleRows(resultRows, ctx)
}

// The cols for the output of an aggregation are:
// one col holding each aggregate result in the order they appear in the select
// one col for each original column from the input in the same order as the input

// There is one agg function for each of the columns above, for aggregate columns,
// it's the actual aggregate function, otherwise it's a "firstrow" function

func (a *Aggregator) calcAggregations(row *common.PushRow, ctx *ExecutionContext, aggStates map[string]*aggfuncs.AggState) error {

	groupByVals := make([]interface{}, len(a.keyCols))
	for i, pkCol := range a.keyCols {
		colType := a.colTypes[pkCol]
		var val interface{}
		null := row.IsNull(pkCol)
		if !null {
			switch colType {
			case common.TypeTinyInt, common.TypeInt, common.TypeBigInt:
				val = row.GetInt64(pkCol)
			case common.TypeDecimal:
				// TODO
			case common.TypeDouble:
				val = row.GetFloat64(pkCol)
			case common.TypeVarchar:
				val = row.GetString(pkCol)
			default:
				return fmt.Errorf("unexpected column type %d", colType)
			}
			groupByVals[i] = val
		}
	}

	// TODO faster encoding that doesn't require creating of []interface{} ??
	var keyBytes []byte
	keyBytes, err := common.EncodeKey(groupByVals, a.colTypes, keyBytes)
	if err != nil {
		return nil
	}

	aggState, ok := aggStates[common.ByteSliceToStringZeroCopy(keyBytes)]
	if !ok {
		currRow, err := a.Table.LookupInPk(groupByVals, ctx.WriteBatch.ShardID)
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
					switch colType {
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
		switch aggFunc.ValueType() {
		case common.TypeTinyInt, common.TypeInt, common.TypeBigInt:
			arg, null, err := aggFunc.ArgExpression().EvalInt64(row)
			if err != nil {
				return err
			}
			err = aggFunc.EvalInt64(arg, null, aggState, index)
			if err != nil {
				return nil
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
				return nil
			}
		case common.TypeVarchar:
			arg, null, err := aggFunc.ArgExpression().EvalString(row)
			if err != nil {
				return err
			}
			err = aggFunc.EvalString(arg, null, aggState, index)
			if err != nil {
				return nil
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
