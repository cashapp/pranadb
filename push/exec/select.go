package exec

import (
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/errors"
)

type PushSelect struct {
	pushExecutorBase
	predicates []*common.Expression
}

func NewPushSelect(predicates []*common.Expression) *PushSelect {
	return &PushSelect{
		pushExecutorBase: pushExecutorBase{},
		predicates:       predicates,
	}
}

func (p *PushSelect) calculateSchema(childColTypes []common.ColumnType, childPkCols []int) {
	p.colTypes = childColTypes
	p.keyCols = childPkCols
	p.rowsFactory = common.NewRowsFactory(p.colTypes)
}

func (p *PushSelect) ReCalcSchemaFromChildren() error {
	if len(p.children) != 1 {
		panic("must be one child")
	}
	child := p.children[0]
	p.calculateSchema(child.ColTypes(), child.KeyCols())
	return nil
}

func (p *PushSelect) HandleRows(rowsBatch RowsBatch, ctx *ExecutionContext) error {
	numRows := rowsBatch.Len()
	resultRows := p.rowsFactory.NewRows(numRows)
	resultBatch := NewCurrentRowsBatch(resultRows)
	for i := 0; i < numRows; i++ {

		currRow := rowsBatch.CurrentRow(i)
		prevRow := rowsBatch.PreviousRow(i)

		if currRow != nil && prevRow == nil {
			// A new row
			ok, err := p.evalPredicates(currRow)
			if err != nil {
				return errors.WithStack(err)
			}
			if ok {
				resultBatch.AppendEntry(nil, currRow)
			}
		} else if currRow != nil && prevRow != nil {
			// A modified row
			okCurr, err := p.evalPredicates(currRow)
			if err != nil {
				return errors.WithStack(err)
			}
			okPrev, err := p.evalPredicates(prevRow)
			if err != nil {
				return errors.WithStack(err)
			}
			if okCurr && okPrev {
				// Both the current and previous version pass the condition so this remains a modify
				resultBatch.AppendEntry(prevRow, currRow)
			} else if !okCurr && okPrev {
				// Previous value passed the filter but current value doesn't so this becomes a delete
				resultBatch.AppendEntry(prevRow, nil)
			} else if okCurr && !okPrev {
				// Passes now but didn't pass before - becomes an add
				resultBatch.AppendEntry(nil, currRow)
			}
		} else if currRow == nil && prevRow != nil {
			// A deleted row - pass it through if it previously was passed
			ok, err := p.evalPredicates(prevRow)
			if err != nil {
				return errors.WithStack(err)
			}
			if ok {
				resultBatch.AppendEntry(prevRow, nil)
			}
		}
	}
	return p.parent.HandleRows(resultBatch, ctx)
}

func (p *PushSelect) evalPredicates(row *common.Row) (bool, error) {
	for _, predicate := range p.predicates {
		accept, isNull, err := predicate.EvalBoolean(row)
		if err != nil {
			return false, errors.WithStack(err)
		}
		if isNull {
			return false, errors.New("null returned from evaluating select predicate")
		}
		if !accept {
			return false, nil
		}
	}
	return true, nil
}
