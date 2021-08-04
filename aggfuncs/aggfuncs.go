package aggfuncs

import (
	"errors"
	"fmt"
	"unsafe"

	"github.com/squareup/pranadb/common"
)

type AggState struct {
	state     []uint64
	strState  *string
	nullFlags []bool
	set       []bool
}

func NewAggState(size int) *AggState {
	return &AggState{
		state:     make([]uint64, size),
		nullFlags: make([]bool, size),
		set:       make([]bool, size),
	}
}

func (as *AggState) IsNull(index int) bool {
	return as.nullFlags[index]
}

func (as *AggState) SetNull(index int, null bool) {
	as.set[index] = true
	as.nullFlags[index] = null
}

func (as *AggState) SetInt64(index int, val int64) {
	as.set[index] = true
	ptrInt64 := (*int64)(unsafe.Pointer(&as.state[index])) // nolint: gosec
	*ptrInt64 = val
}

func (as *AggState) GetInt64(index int) int64 {
	ptrInt64 := (*int64)(unsafe.Pointer(&as.state[index])) // nolint: gosec
	return *ptrInt64
}

func (as *AggState) SetFloat64(index int, val float64) {
	as.set[index] = true
	ptrFloat64 := (*float64)(unsafe.Pointer(&as.state[index])) // nolint: gosec
	*ptrFloat64 = val
}

func (as *AggState) GetFloat64(index int) float64 {
	ptrFloat64 := (*float64)(unsafe.Pointer(&as.state[index])) // nolint: gosec
	return *ptrFloat64
}

func (as *AggState) SetString(index int, val *string) {
	as.set[index] = true
	as.strState = val
}

func (as *AggState) GetString(index int) *string {
	return as.strState
}

func (as *AggState) SetTimestamp(index int, val common.Timestamp) error {
	packed, err := val.ToPackedUint()
	if err != nil {
		return err
	}
	as.SetInt64(index, int64(packed))
	return nil
}

func (as *AggState) GetTimestamp(index int) (common.Timestamp, error) {
	packed := as.GetInt64(index)
	ts := common.Timestamp{}
	if err := ts.FromPackedUint(uint64(packed)); err != nil {
		return common.Timestamp{}, err
	}
	return ts, nil
}

func (as *AggState) IsSet(index int) bool {
	return as.set[index]
}

// AggregateFunction functions are stateful so you can update it multiple times before getting value
type AggregateFunction interface {
	EvalInt64(currValue int64, null bool, aggState *AggState, index int) error
	EvalFloat64(currValue float64, null bool, aggState *AggState, index int) error
	EvalString(currValue string, null bool, aggState *AggState, index int) error
	EvalTimestamp(currValue common.Timestamp, null bool, aggState *AggState, index int) error

	ValueType() common.ColumnType
	ArgExpression() *common.Expression
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
	case AverageAggregateFunctionType:
		return nil, errors.New("AverageAggregateFunctionType not implemented")
	case FirstRowAggregateFunctionType:
		return &FirstRowAggregateFunction{aggregateFunctionBase: base}, nil
	default:
		return nil, fmt.Errorf("unexpected aggregate function type %d", funcType)
	}
}

type SumAggregateFunction struct {
	aggregateFunctionBase
}

func (s *SumAggregateFunction) EvalInt64(currValue int64, null bool, aggState *AggState, index int) error {
	if null {
		return nil
	}
	aggState.SetInt64(index, aggState.GetInt64(index)+currValue)
	return nil
}

func (s *SumAggregateFunction) EvalFloat64(currValue float64, null bool, aggState *AggState, index int) error {
	if null {
		return nil
	}
	aggState.SetFloat64(index, aggState.GetFloat64(index)+currValue)
	return nil
}

func (s *SumAggregateFunction) EvalString(currValue string, null bool, aggState *AggState, index int) error {
	if null {
		return nil
	}
	panic("implement me")
}

func (s *SumAggregateFunction) EvalTimestamp(currValue common.Timestamp, null bool, aggState *AggState, index int) error {
	panic("cannot sum on timestamp")
}

type CountAggregateFunction struct {
	aggregateFunctionBase
}

func (s *CountAggregateFunction) EvalInt64(currValue int64, null bool, aggState *AggState, index int) error {
	if null {
		return nil
	}
	aggState.SetInt64(index, aggState.GetInt64(index)+1)
	return nil
}

func (s *CountAggregateFunction) EvalFloat64(currValue float64, null bool, aggState *AggState, index int) error {
	if null {
		return nil
	}
	aggState.SetInt64(index, aggState.GetInt64(index)+1)
	return nil
}

func (s *CountAggregateFunction) EvalString(currValue string, null bool, aggState *AggState, index int) error {
	if null {
		return nil
	}
	aggState.SetInt64(index, aggState.GetInt64(index)+1)
	return nil
}

func (s *CountAggregateFunction) EvalTimestamp(currValue common.Timestamp, null bool, aggState *AggState, index int) error {
	if null {
		return nil
	}
	aggState.SetInt64(index, aggState.GetInt64(index)+1)
	return nil
}

type FirstRowAggregateFunction struct {
	aggregateFunctionBase
}

func (f *FirstRowAggregateFunction) EvalInt64(currValue int64, null bool, aggState *AggState, index int) error {
	if aggState.IsSet(index) {
		return nil
	}
	if null {
		aggState.SetNull(index, true)
	} else {
		aggState.SetInt64(index, currValue)
	}
	return nil
}

func (f *FirstRowAggregateFunction) EvalFloat64(currValue float64, null bool, aggState *AggState, index int) error {
	if aggState.IsSet(index) {
		return nil
	}
	if null {
		aggState.SetNull(index, true)
	} else {
		aggState.SetFloat64(index, currValue)
	}
	return nil
}

func (f *FirstRowAggregateFunction) EvalString(currValue string, null bool, aggState *AggState, index int) error {
	if aggState.IsSet(index) {
		return nil
	}
	if null {
		aggState.SetNull(index, true)
	} else {
		aggState.SetString(index, &currValue)
	}
	return nil
}

func (f *FirstRowAggregateFunction) EvalTimestamp(currValue common.Timestamp, null bool, aggState *AggState, index int) error {
	if aggState.IsSet(index) {
		return nil
	}
	if null {
		aggState.SetNull(index, true)
	} else if err := aggState.SetTimestamp(index, currValue); err != nil {
		return err
	}
	return nil
}

type MaxAggregateFunction struct {
	aggregateFunctionBase
}

func (m *MaxAggregateFunction) EvalInt64(currValue int64, null bool, aggState *AggState, index int) error {
	if null {
		return nil
	}
	if !aggState.IsSet(index) || (currValue > aggState.GetInt64(index)) {
		aggState.SetInt64(index, currValue)
	}
	return nil
}

func (m *MaxAggregateFunction) EvalFloat64(currValue float64, null bool, aggState *AggState, index int) error {
	if null {
		return nil
	}
	if !aggState.IsSet(index) || (currValue > aggState.GetFloat64(index)) {
		aggState.SetFloat64(index, currValue)
	}
	return nil
}

func (m *MaxAggregateFunction) EvalString(currValue string, null bool, aggState *AggState, index int) error {
	panic("should not be called")
}

func (m *MaxAggregateFunction) EvalTimestamp(currValue common.Timestamp, null bool, aggState *AggState, index int) error {
	if null {
		return nil
	}
	other, err := aggState.GetTimestamp(index)
	if err != nil {
		return err
	}
	if !aggState.IsSet(index) || (currValue.Compare(other) > 1) {
		if err := aggState.SetTimestamp(index, currValue); err != nil {
			return err
		}
	}
	return nil
}

type MinAggregateFunction struct {
	aggregateFunctionBase
}

func (m *MinAggregateFunction) EvalInt64(currValue int64, null bool, aggState *AggState, index int) error {
	if null {
		return nil
	}
	if !aggState.IsSet(index) || (currValue < aggState.GetInt64(index)) {
		aggState.SetInt64(index, currValue)
	}
	return nil
}

func (m *MinAggregateFunction) EvalFloat64(currValue float64, null bool, aggState *AggState, index int) error {
	if null {
		return nil
	}
	if !aggState.IsSet(index) || (currValue < aggState.GetFloat64(index)) {
		aggState.SetFloat64(index, currValue)
	}
	return nil
}

func (m *MinAggregateFunction) EvalString(currValue string, null bool, aggState *AggState, index int) error {
	panic("should not be called")
}

func (m *MinAggregateFunction) EvalTimestamp(currValue common.Timestamp, null bool, aggState *AggState, index int) error {
	if null {
		return nil
	}
	other, err := aggState.GetTimestamp(index)
	if err != nil {
		return err
	}
	if !aggState.IsSet(index) || (currValue.Compare(other) < 1) {
		if err := aggState.SetTimestamp(index, currValue); err != nil {
			return err
		}
	}
	return nil
}
