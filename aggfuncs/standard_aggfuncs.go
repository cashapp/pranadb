package aggfuncs

import (
	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/common"
)

// SUM
// ===

type SumAggregateFunction struct {
	aggregateFunctionBase
}

func (s *SumAggregateFunction) MergeDecimal(prevMergeState *AggState, currMergeState *AggState, aggState *AggState, index int) error {
	var prev *common.Decimal
	var curr *common.Decimal
	if prevMergeState == nil {
		prev = common.ZeroDecimal()
	} else {
		p := prevMergeState.GetDecimal(index)
		prev = &p
	}
	if currMergeState == nil {
		curr = common.ZeroDecimal()
	} else {
		c := currMergeState.GetDecimal(index)
		curr = &c
	}
	res, err := curr.Subtract(prev)
	if err != nil {
		return err
	}
	return s.EvalDecimal(*res, false, aggState, index)
}

func (s *SumAggregateFunction) MergeFloat64(prevMergeState *AggState, currMergeState *AggState, aggState *AggState, index int) error {
	var prev float64
	var curr float64
	if prevMergeState == nil {
		prev = 0
	} else {
		prev = prevMergeState.GetFloat64(index)
	}
	if currMergeState == nil {
		curr = 0
	} else {
		curr = currMergeState.GetFloat64(index)
	}
	return s.EvalFloat64(curr-prev, false, aggState, index)
}

func (s *SumAggregateFunction) EvalDecimal(currValue common.Decimal, null bool, aggState *AggState, index int) error {
	if null {
		return nil
	}
	if !aggState.IsSet(index) {
		return aggState.SetDecimal(index, currValue)
	}
	curr := aggState.GetDecimal(index)
	res, err := curr.Add(&currValue)
	if err != nil {
		return err
	}
	return aggState.SetDecimal(index, *res)
}

func (s *SumAggregateFunction) EvalFloat64(currValue float64, null bool, aggState *AggState, index int) error {
	if null {
		return nil
	}
	aggState.SetFloat64(index, aggState.GetFloat64(index)+currValue)
	return nil
}

type CountAggregateFunction struct {
	aggregateFunctionBase
}

// COUNT
// =====

func (s *CountAggregateFunction) MergeInt64(prevMergeState *AggState, currMergeState *AggState, aggState *AggState, index int) error {
	var prev, curr int64
	if prevMergeState == nil {
		prev = 0
	} else {
		prev = prevMergeState.GetInt64(index)
	}
	if currMergeState == nil {
		curr = 0
	} else {
		curr = currMergeState.GetInt64(index)
	}
	aggState.SetInt64(index, aggState.GetInt64(index)+curr-prev)
	return nil
}

func (s *CountAggregateFunction) EvalInt64(currValue int64, null bool, aggState *AggState, index int) error {
	if null {
		return nil
	}
	aggState.SetInt64(index, aggState.GetInt64(index)+1)
	return nil
}

// MAX
// ===

type MaxAggregateFunction struct {
	aggregateFunctionBase
}

func checkDelete(prevMergeState *AggState, currMergeState *AggState, funcName string) bool {
	if prevMergeState != nil && currMergeState == nil {
		// We have a delete
		log.Warnf("Reversal of %s aggregation function not implemented", funcName)
		return false
	}
	return true
}

func (m *MaxAggregateFunction) MergeInt64(prevMergeState *AggState, currMergeState *AggState, aggState *AggState, index int) error {
	if !checkDelete(prevMergeState, currMergeState, "max") {
		return nil
	}
	if currMergeState != nil {
		// The latest value is always correct
		return m.EvalInt64(currMergeState.GetInt64(index), false, aggState, index)
	}
	return nil
}

func (m *MaxAggregateFunction) MergeFloat64(prevMergeState *AggState, currMergeState *AggState, aggState *AggState, index int) error {
	if !checkDelete(prevMergeState, currMergeState, "max") {
		return nil
	}
	if currMergeState != nil {
		// The latest value is always correct
		return m.EvalFloat64(currMergeState.GetFloat64(index), false, aggState, index)
	}
	return nil
}

func (m *MaxAggregateFunction) MergeTimestamp(prevMergeState *AggState, currMergeState *AggState, aggState *AggState, index int) error {
	if !checkDelete(prevMergeState, currMergeState, "max") {
		return nil
	}
	if currMergeState != nil {
		// The latest value is always correct
		currTS, err := currMergeState.GetTimestamp(index)
		if err != nil {
			return err
		}
		return m.EvalTimestamp(currTS, false, aggState, index)
	}
	return nil
}

func (m *MaxAggregateFunction) MergeDecimal(prevMergeState *AggState, currMergeState *AggState, aggState *AggState, index int) error {
	if !checkDelete(prevMergeState, currMergeState, "max") {
		return nil
	}
	if currMergeState != nil {
		// The latest value is always correct
		return m.EvalDecimal(currMergeState.GetDecimal(index), false, aggState, index)
	}
	return nil
}

func (m *MaxAggregateFunction) EvalInt64(currValue int64, null bool, aggState *AggState, index int) error {
	if null {
		return nil
	}
	prevState := aggState.GetInt64(index)
	if !aggState.IsSet(index) || (currValue > prevState) {
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

func (m *MaxAggregateFunction) EvalTimestamp(currValue common.Timestamp, null bool, aggState *AggState, index int) error {
	if null {
		return nil
	}
	other, err := aggState.GetTimestamp(index)
	if err != nil {
		return err
	}
	if !aggState.IsSet(index) || (currValue.Compare(other) > 0) {
		if err := aggState.SetTimestamp(index, currValue); err != nil {
			return err
		}
	}
	return nil
}

func (m *MaxAggregateFunction) EvalDecimal(currValue common.Decimal, null bool, aggState *AggState, index int) error {
	if null {
		return nil
	}
	other := aggState.GetDecimal(index)
	if !aggState.IsSet(index) || (currValue.CompareTo(&other) > 0) {
		if err := aggState.SetDecimal(index, currValue); err != nil {
			return err
		}
	}
	return nil
}

// MIN
// ===

type MinAggregateFunction struct {
	aggregateFunctionBase
}

func (m *MinAggregateFunction) MergeInt64(prevMergeState *AggState, currMergeState *AggState, aggState *AggState, index int) error {
	if !checkDelete(prevMergeState, currMergeState, "min") {
		return nil
	}
	if currMergeState != nil {
		// The latest value is always correct
		return m.EvalInt64(currMergeState.GetInt64(index), false, aggState, index)
	}
	return nil
}

func (m *MinAggregateFunction) MergeFloat64(prevMergeState *AggState, currMergeState *AggState, aggState *AggState, index int) error {
	if !checkDelete(prevMergeState, currMergeState, "min") {
		return nil
	}
	if currMergeState != nil {
		// The latest value is always correct
		return m.EvalFloat64(currMergeState.GetFloat64(index), false, aggState, index)
	}
	return nil
}

func (m *MinAggregateFunction) MergeTimestamp(prevMergeState *AggState, currMergeState *AggState, aggState *AggState, index int) error {
	if !checkDelete(prevMergeState, currMergeState, "min") {
		return nil
	}
	if currMergeState != nil {
		// The latest value is always correct
		currTS, err := currMergeState.GetTimestamp(index)
		if err != nil {
			return err
		}
		return m.EvalTimestamp(currTS, false, aggState, index)
	}
	return nil
}

func (m *MinAggregateFunction) MergeDecimal(prevMergeState *AggState, currMergeState *AggState, aggState *AggState, index int) error {
	if !checkDelete(prevMergeState, currMergeState, "min") {
		return nil
	}
	if currMergeState != nil {
		// The latest value is always correct
		return m.EvalDecimal(currMergeState.GetDecimal(index), false, aggState, index)
	}
	return nil
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

func (m *MinAggregateFunction) EvalTimestamp(currValue common.Timestamp, null bool, aggState *AggState, index int) error {
	if null {
		return nil
	}
	other, err := aggState.GetTimestamp(index)
	if err != nil {
		return err
	}
	if !aggState.IsSet(index) || (currValue.Compare(other) < 0) {
		if err := aggState.SetTimestamp(index, currValue); err != nil {
			return err
		}
	}
	return nil
}

func (m *MinAggregateFunction) EvalDecimal(currValue common.Decimal, null bool, aggState *AggState, index int) error {
	if null {
		return nil
	}
	other := aggState.GetDecimal(index)
	if !aggState.IsSet(index) || (currValue.CompareTo(&other) < 0) {
		if err := aggState.SetDecimal(index, currValue); err != nil {
			return err
		}
	}
	return nil
}

// FIRSTROW
// ========

type FirstRowAggregateFunction struct {
	aggregateFunctionBase
}

func (f *FirstRowAggregateFunction) MergeInt64(prevMergeState *AggState, currMergeState *AggState, aggState *AggState, index int) error {
	if aggState.IsSet(index) {
		return nil
	}
	if currMergeState.IsNull(index) {
		aggState.SetNull(index)
	} else {
		aggState.SetInt64(index, currMergeState.GetInt64(index))
	}
	return nil
}

func (f *FirstRowAggregateFunction) MergeFloat64(prevMergeState *AggState, currMergeState *AggState, aggState *AggState, index int) error {
	if aggState.IsSet(index) {
		return nil
	}
	if currMergeState.IsNull(index) {
		aggState.SetNull(index)
	} else {
		aggState.SetFloat64(index, currMergeState.GetFloat64(index))
	}
	return nil
}

func (f *FirstRowAggregateFunction) MergeString(prevMergeState *AggState, currMergeState *AggState, aggState *AggState, index int) error {
	if aggState.IsSet(index) {
		return nil
	}
	if currMergeState.IsNull(index) {
		aggState.SetNull(index)
	} else {
		aggState.SetString(index, currMergeState.GetString(index))
	}
	return nil
}

func (f *FirstRowAggregateFunction) MergeTimestamp(prevMergeState *AggState, currMergeState *AggState, aggState *AggState, index int) error {
	if aggState.IsSet(index) {
		return nil
	}
	if currMergeState.IsNull(index) {
		aggState.SetNull(index)
	} else {
		currTS, err := currMergeState.GetTimestamp(index)
		if err != nil {
			return err
		}
		if err := aggState.SetTimestamp(index, currTS); err != nil {
			return err
		}
	}
	return nil
}

func (f *FirstRowAggregateFunction) MergeDecimal(prevMergeState *AggState, currMergeState *AggState, aggState *AggState, index int) error {
	if aggState.IsSet(index) {
		return nil
	}
	if currMergeState.IsNull(index) {
		aggState.SetNull(index)
	} else if err := aggState.SetDecimal(index, currMergeState.GetDecimal(index)); err != nil {
		return err
	}
	return nil
}

func (f *FirstRowAggregateFunction) EvalInt64(currValue int64, null bool, aggState *AggState, index int) error {
	if aggState.IsSet(index) {
		return nil
	}
	if null {
		aggState.SetNull(index)
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
		aggState.SetNull(index)
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
		aggState.SetNull(index)
	} else {
		aggState.SetString(index, currValue)
	}
	return nil
}

func (f *FirstRowAggregateFunction) EvalTimestamp(currValue common.Timestamp, null bool, aggState *AggState, index int) error {
	if aggState.IsSet(index) {
		return nil
	}
	if null {
		aggState.SetNull(index)
	} else if err := aggState.SetTimestamp(index, currValue); err != nil {
		return err
	}
	return nil
}

func (f *FirstRowAggregateFunction) EvalDecimal(currValue common.Decimal, null bool, aggState *AggState, index int) error {
	if aggState.IsSet(index) {
		return nil
	}
	if null {
		aggState.SetNull(index)
	} else if err := aggState.SetDecimal(index, currValue); err != nil {
		return err
	}
	return nil
}
