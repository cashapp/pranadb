package exec

import (
	"errors"
	"github.com/squareup/pranadb/common"
)

type PushSelect struct {
	pushExecutorBase
	predicates []*common.Expression
}

func NewPushSelect(colNames []string, colTypes []common.ColumnType, predicates []*common.Expression) (*PushSelect, error) {
	rf, err := common.NewRowsFactory(colTypes)
	if err != nil {
		return nil, err
	}
	pushBase := pushExecutorBase{
		colNames:    colNames,
		colTypes:    colTypes,
		rowsFactory: rf,
	}
	return &PushSelect{
		pushExecutorBase: pushBase,
		predicates:       predicates,
	}, nil
}

func (p *PushSelect) ReCalcSchemaFromChildren() {
	if len(p.children) > 1 {
		panic("too many children")
	}
	if len(p.children) == 1 {
		child := p.children[0]
		p.colNames = child.ColNames()
		p.colTypes = child.ColTypes()
		p.keyCols = child.KeyCols()
	}
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
