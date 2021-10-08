package aggfuncs

import (
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/errors"
)

type AggregateFunction interface {
	EvalInt64(currValue int64, null bool, aggState *AggState, index int) error
	EvalFloat64(currValue float64, null bool, aggState *AggState, index int) error
	EvalString(currValue string, null bool, aggState *AggState, index int) error
	EvalTimestamp(currValue common.Timestamp, null bool, aggState *AggState, index int) error
	EvalDecimal(currValue common.Decimal, null bool, aggState *AggState, index int) error

	MergeInt64(prevMergeState *AggState, currMergeState *AggState, aggState *AggState, index int) error
	MergeFloat64(prevMergeState *AggState, currMergeState *AggState, aggState *AggState, index int) error
	MergeString(prevMergeState *AggState, currMergeState *AggState, aggState *AggState, index int) error
	MergeTimestamp(prevMergeState *AggState, currMergeState *AggState, aggState *AggState, index int) error
	MergeDecimal(prevMergeState *AggState, currMergeState *AggState, aggState *AggState, index int) error

	ValueType() common.ColumnType
	ArgExpression() *common.Expression
	RequiresExtraState() bool
}

type aggregateFunctionBase struct {
	argExpression *common.Expression
	valueType     common.ColumnType
}

type AggFunctionType int

const (
	SumAggregateFunctionType AggFunctionType = iota
	CountAggregateFunctionType
	AverageAggregateFunctionType
	MaxAggregateFunctionType
	MinAggregateFunctionType
	FirstRowAggregateFunctionType
)

func (b *aggregateFunctionBase) ValueType() common.ColumnType {
	return b.valueType
}

func (b *aggregateFunctionBase) ArgExpression() *common.Expression {
	return b.argExpression
}

func (b *aggregateFunctionBase) RequiresExtraState() bool {
	return false
}

func NewAggregateFunction(argExpression *common.Expression, funcType AggFunctionType, valueType common.ColumnType) (AggregateFunction, error) {
	base := aggregateFunctionBase{argExpression: argExpression, valueType: valueType}
	switch funcType {
	case SumAggregateFunctionType:
		return &SumAggregateFunction{aggregateFunctionBase: base}, nil
	case CountAggregateFunctionType:
		return &CountAggregateFunction{aggregateFunctionBase: base}, nil
	case MaxAggregateFunctionType:
		return &MaxAggregateFunction{aggregateFunctionBase: base}, nil
	case MinAggregateFunctionType:
		return &MinAggregateFunction{aggregateFunctionBase: base}, nil
	case FirstRowAggregateFunctionType:
		return &FirstRowAggregateFunction{aggregateFunctionBase: base}, nil
	default:
		return nil, errors.Errorf("unexpected aggregate function type %d", funcType)
	}
}

func (b *aggregateFunctionBase) EvalInt64(currValue int64, null bool, aggState *AggState, index int) error {
	panic("should not be called")
}

func (b *aggregateFunctionBase) EvalFloat64(currValue float64, null bool, aggState *AggState, index int) error {
	panic("should not be called")
}

func (b *aggregateFunctionBase) EvalString(currValue string, null bool, aggState *AggState, index int) error {
	panic("should not be called")
}

func (b *aggregateFunctionBase) EvalTimestamp(currValue common.Timestamp, null bool, aggState *AggState, index int) error {
	panic("should not be called")
}

func (b *aggregateFunctionBase) EvalDecimal(currValue common.Decimal, null bool, aggState *AggState, index int) error {
	panic("should not be called")
}

func (b *aggregateFunctionBase) MergeInt64(prevMergeState *AggState, currMergeState *AggState, aggState *AggState, index int) error {
	panic("should not be called")
}

func (b *aggregateFunctionBase) MergeFloat64(prevMergeState *AggState, currMergeState *AggState, aggState *AggState, index int) error {
	panic("should not be called")
}

func (b *aggregateFunctionBase) MergeString(prevMergeState *AggState, currMergeState *AggState, aggState *AggState, index int) error {
	panic("should not be called")
}

func (b *aggregateFunctionBase) MergeTimestamp(prevMergeState *AggState, currMergeState *AggState, aggState *AggState, index int) error {
	panic("should not be called")
}

func (b *aggregateFunctionBase) MergeDecimal(prevMergeState *AggState, currMergeState *AggState, aggState *AggState, index int) error {
	panic("should not be called")
}
