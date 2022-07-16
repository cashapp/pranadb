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
