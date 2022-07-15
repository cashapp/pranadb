package aggfuncs

import (
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/errors"
)

type AggregateFunction interface {
	EvalInt64(value int64, null bool, aggState *AggState, index int, reverse bool) error
	EvalFloat64(value float64, null bool, aggState *AggState, index int, reverse bool) error
	EvalString(value string, null bool, aggState *AggState, index int, reverse bool) error
	EvalTimestamp(value common.Timestamp, null bool, aggState *AggState, index int, reverse bool) error
	EvalDecimal(value common.Decimal, null bool, aggState *AggState, index int, reverse bool) error

	ValueType() common.ColumnType
	ArgExpression() *common.Expression
	RequiresExtraState() bool
}

type aggregateFunctionBase struct {
	argExpression *common.Expression
	valueType     common.ColumnType
}

func (b *aggregateFunctionBase) EvalInt64(value int64, null bool, aggState *AggState, index int, reverse bool) error {
	panic("should not be called")
}

func (b *aggregateFunctionBase) EvalFloat64(value float64, null bool, aggState *AggState, index int, reverse bool) error {
	panic("should not be called")
}

func (b *aggregateFunctionBase) EvalString(value string, null bool, aggState *AggState, index int, reverse bool) error {
	panic("should not be called")
}

func (b *aggregateFunctionBase) EvalTimestamp(value common.Timestamp, null bool, aggState *AggState, index int, reverse bool) error {
	panic("should not be called")
}

func (b *aggregateFunctionBase) EvalDecimal(value common.Decimal, null bool, aggState *AggState, index int, reverse bool) error {
	panic("should not be called")
}

type AggFunctionType int

const (
	SumAggregateFunctionType AggFunctionType = iota
	CountAggregateFunctionType
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
	case FirstRowAggregateFunctionType:
		return &FirstRowAggregateFunction{aggregateFunctionBase: base}, nil
	default:
		return nil, errors.Errorf("unexpected aggregate function type %d", funcType)
	}
}
