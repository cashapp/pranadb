package exec

import (
	"errors"

	"github.com/squareup/pranadb/common"
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

func (p *PushSelect) calculateSchema(childColNames []string, childColTypes []common.ColumnType, childPkCols []int) {
	p.colNames = childColNames
	p.colTypes = childColTypes
	p.keyCols = childPkCols
	p.rowsFactory = common.NewRowsFactory(p.colTypes)
}

func (p *PushSelect) ReCalcSchemaFromChildren() error {
	if len(p.children) != 1 {
		panic("must be one child")
	}
	child := p.children[0]
	p.calculateSchema(child.ColNames(), child.ColTypes(), child.KeyCols())
	return nil
}

func (p *PushSelect) HandleRows(rows *common.Rows, ctx *ExecutionContext) error {
	result := p.rowsFactory.NewRows(rows.RowCount())
	for i := 0; i < rows.RowCount(); i++ {
		row := rows.GetRow(i)
		ok := true
		for _, predicate := range p.predicates {
			accept, isNull, err := predicate.EvalBoolean(&row)
			if err != nil {
				return err
			}
			if isNull {
				return errors.New("null returned from evaluating select predicate")
			}
			if !accept {
				ok = false
				break
			}
		}
		if ok {
			result.AppendRow(row)
		}
	}
	return p.parent.HandleRows(result, ctx)
}
