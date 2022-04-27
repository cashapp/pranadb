package aggfuncs

import (
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/errors"
)

// SUM
// ===

type SumAggregateFunction struct {
	aggregateFunctionBase
}

func (s *SumAggregateFunction) EvalFloat64(value float64, null bool, aggState *AggState, index int, reverse bool) error {
	if null {
		return nil
	}
	currVal := aggState.GetFloat64(index)
	var newVal float64
	if reverse {
		newVal = currVal - value
	} else {
		newVal = currVal + value
	}
	aggState.SetFloat64(index, newVal)
	return nil
}

func (s *SumAggregateFunction) EvalDecimal(value common.Decimal, null bool, aggState *AggState, index int, reverse bool) error {
	if null {
		return nil
	}
	currVal := aggState.GetDecimal(index)
	var newVal *common.Decimal
	var err error
	if reverse {
		newVal, err = currVal.Subtract(&value)
		if err != nil {
			return errors.WithStack(err)
		}
	} else {
		newVal, err = currVal.Add(&value)
		if err != nil {
			return errors.WithStack(err)
		}
	}
	return aggState.SetDecimal(index, *newVal)
}

func (s *SumAggregateFunction) MergeFloat64(latestState *AggState, aggState *AggState, index int, reverse bool) error {
	value := latestState.GetFloat64(index)
	return s.EvalFloat64(value, false, aggState, index, reverse)
}

func (s *SumAggregateFunction) MergeDecimal(latestState *AggState, aggState *AggState, index int, reverse bool) error {
	value := latestState.GetDecimal(index)
	return s.EvalDecimal(value, false, aggState, index, reverse)
}

// COUNT
// =====

type CountAggregateFunction struct {
	aggregateFunctionBase
}

func (c *CountAggregateFunction) EvalInt64(value int64, null bool, aggState *AggState, index int, reverse bool) error {
	if null {
		return nil
	}
	currVal := aggState.GetInt64(index)
	var newVal int64
	if reverse {
		newVal = currVal - 1
	} else {
		newVal = currVal + 1
	}
	aggState.SetInt64(index, newVal)
	return nil
}

func (c *CountAggregateFunction) MergeInt64(toMerge *AggState, aggState *AggState, index int, reverse bool) error {
	value := toMerge.GetInt64(index)
	currVal := aggState.GetInt64(index)
	var newVal int64
	if reverse {
		newVal = currVal - value
	} else {
		newVal = currVal + value
	}
	aggState.SetInt64(index, newVal)
	return nil
}

// FIRSTROW
// ========

type FirstRowAggregateFunction struct {
	aggregateFunctionBase
}

func (f *FirstRowAggregateFunction) EvalInt64(value int64, null bool, aggState *AggState, index int, reverse bool) error {
	if aggState.IsSet(index) {
		return nil
	}
	if null {
		aggState.SetNull(index)
	} else {
		aggState.SetInt64(index, value)
	}
	return nil
}

func (f *FirstRowAggregateFunction) EvalFloat64(value float64, null bool, aggState *AggState, index int, reverse bool) error {
	if aggState.IsSet(index) {
		return nil
	}
	if null {
		aggState.SetNull(index)
	} else {
		aggState.SetFloat64(index, value)
	}
	return nil
}

func (f *FirstRowAggregateFunction) EvalString(value string, null bool, aggState *AggState, index int, reverse bool) error {
	if aggState.IsSet(index) {
		return nil
	}
	if null {
		aggState.SetNull(index)
	} else {
		aggState.SetString(index, value)
	}
	return nil
}

func (f *FirstRowAggregateFunction) EvalTimestamp(value common.Timestamp, null bool, aggState *AggState, index int, reverse bool) error {
	if aggState.IsSet(index) {
		return nil
	}
	if null {
		aggState.SetNull(index)
	} else if err := aggState.SetTimestamp(index, value); err != nil {
		return err
	}
	return nil
}

func (f *FirstRowAggregateFunction) EvalDecimal(value common.Decimal, null bool, aggState *AggState, index int, reverse bool) error {
	if aggState.IsSet(index) {
		return nil
	}
	if null {
		aggState.SetNull(index)
	} else if err := aggState.SetDecimal(index, value); err != nil {
		return err
	}
	return nil
}

func (f *FirstRowAggregateFunction) MergeInt64(latestState *AggState, aggState *AggState, index int, reverse bool) error {
	if aggState.IsSet(index) {
		return nil
	}
	if latestState.IsNull(index) {
		aggState.SetNull(index)
	} else {
		aggState.SetInt64(index, latestState.GetInt64(index))
	}
	return nil
}

func (f *FirstRowAggregateFunction) MergeFloat64(latestState *AggState, aggState *AggState, index int, reverse bool) error {
	if aggState.IsSet(index) {
		return nil
	}
	if latestState.IsNull(index) {
		aggState.SetNull(index)
	} else {
		aggState.SetFloat64(index, latestState.GetFloat64(index))
	}
	return nil
}

func (f *FirstRowAggregateFunction) MergeString(latestState *AggState, aggState *AggState, index int, reverse bool) error {
	if aggState.IsSet(index) {
		return nil
	}
	if latestState.IsNull(index) {
		aggState.SetNull(index)
	} else {
		aggState.SetString(index, latestState.GetString(index))
	}
	return nil
}

func (f *FirstRowAggregateFunction) MergeDecimal(latestState *AggState, aggState *AggState, index int, reverse bool) error {
	if aggState.IsSet(index) {
		return nil
	}
	if latestState.IsNull(index) {
		aggState.SetNull(index)
	} else {
		return aggState.SetDecimal(index, latestState.GetDecimal(index))
	}
	return nil
}

func (f *FirstRowAggregateFunction) MergeTimestamp(latestState *AggState, aggState *AggState, index int, reverse bool) error {
	if aggState.IsSet(index) {
		return nil
	}
	if latestState.IsNull(index) {
		aggState.SetNull(index)
	} else {
		ts, err := latestState.GetTimestamp(index)
		if err != nil {
			return err
		}
		return aggState.SetTimestamp(index, ts)
	}
	return nil
}
