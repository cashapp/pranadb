package exec

import (
	"errors"
	"github.com/squareup/pranadb/common"
)

type PullSelect struct {
	pullExecutorBase
	predicates []*common.Expression
}

func NewPullSelect(colNames []string, colTypes []common.ColumnType, predicates []*common.Expression) (*PullSelect, error) {
	rf, err := common.NewRowsFactory(colTypes)
	if err != nil {
		return nil, err
	}
	base := pullExecutorBase{
		executorBase: executorBase{
			colNames:    colNames,
			colTypes:    colTypes,
			rowsFactory: rf,
		},
	}
	return &PullSelect{
		pullExecutorBase: base,
		predicates:       predicates,
	}, nil
}

func (p *PullSelect) GetRows(limit int) (rows *common.Rows, err error) {

	rows, err = p.GetChildren()[0].GetRows(limit)
	if err != nil {
		return nil, err
	}
	result := p.rowsFactory.NewRows(rows.RowCount())
	// TODO duplicated logic from push select
	for i := 0; i < rows.RowCount(); i++ {
		row := rows.GetRow(i)
		ok := true
		for _, predicate := range p.predicates {
			accept, isNull, err := predicate.EvalBoolean(&row)
			if err != nil {
				return nil, err
			}
			if isNull {
				return nil, errors.New("null returned from evaluating select predicate")
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

	return result, nil
}
