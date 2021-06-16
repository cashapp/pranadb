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
	base := pushExecutorBase{
		colNames:    colNames,
		colTypes:    colTypes,
		rowsFactory: rf,
	}
	return &PushSelect{
		pushExecutorBase: base,
		predicates:       predicates,
	}, nil
}

func (p *PushSelect) ReCalcSchema() {
	if len(p.children) > 1 {
		panic("too many children")
	}
	if len(p.children) == 1 {
		child := p.children[0]
		child.ReCalcSchema()
		p.ReCalcSchemaFromSources(child.ColNames(), child.ColTypes(), child.KeyCols())
	}
}

func (p *PushSelect) ReCalcSchemaFromSources(colNames []string, colTypes []common.ColumnType, keyCols []int) {
	p.keyCols = keyCols
	p.colNames = colNames
	p.colTypes = colTypes
}

func (p *PushSelect) HandleRows(rows *common.PushRows, ctx *ExecutionContext) error {
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
	return p.parent.HandleRows(rows, ctx)
}
