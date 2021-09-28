package exec

import (
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/common"
)

type PullSelect struct {
	pullExecutorBase
	predicates []*common.Expression
}

func NewPullSelect(colNames []string, colTypes []common.ColumnType, predicates []*common.Expression) *PullSelect {
	rf := common.NewRowsFactory(colTypes)
	base := pullExecutorBase{
		colNames:       colNames,
		colTypes:       colTypes,
		simpleColNames: common.ToSimpleColNames(colNames),
		rowsFactory:    rf,
	}
	return &PullSelect{
		pullExecutorBase: base,
		predicates:       predicates,
	}
}

func (p *PullSelect) GetRows(limit int) (rows *common.Rows, err error) {
	rows, err = p.getRows(limit)
	if err != nil {
		log.Printf("Failed to get rows in pullselect %+v", err)
	}
	return rows, err
}

func (p *PullSelect) getRows(limit int) (rows *common.Rows, err error) {

	if limit < 1 {
		return nil, fmt.Errorf("invalid limit %d", limit)
	}

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
