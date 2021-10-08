package exec

import (
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/errors"
)

const batchSize = 10000

type PullSelect struct {
	pullExecutorBase
	predicates    []*common.Expression
	availRows     *common.Rows
	moreInputRows bool
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
		moreInputRows:    true,
	}
}

func (p *PullSelect) GetRows(limit int) (rows *common.Rows, err error) {

	if limit < 1 {
		return nil, errors.Errorf("invalid limit %d", limit)
	}

	var capac int
	if limit < batchSize {
		capac = limit
	} else {
		capac = batchSize
	}

	if p.availRows == nil {
		p.availRows = p.rowsFactory.NewRows(capac)
	}

	rc := p.availRows.RowCount()

	if rc < limit && p.moreInputRows {
		for {
			rows, err = p.GetChildren()[0].GetRows(batchSize)
			if err != nil {
				return nil, err
			}
			numRows := rows.RowCount()

			for i := 0; i < numRows; i++ {
				row := rows.GetRow(i)
				ok := true
				for _, predicate := range p.predicates {
					accept, isNull, err := predicate.EvalBoolean(&row)
					if err != nil {
						return nil, err
					}
					if isNull || !accept {
						ok = false
						break
					}
				}
				if ok {
					p.availRows.AppendRow(row)
				}
			}
			if numRows < batchSize {
				// There are no more rows in the input
				p.moreInputRows = false
				break
			}
			if p.availRows.RowCount() >= limit {
				// We have enough to return
				break
			}
		}
	}

	if p.availRows.RowCount() < limit {
		res := p.availRows
		p.availRows = nil
		return res, nil
	}

	// We have at least limit rows available, we return limit of them and keep the rest in case GetRows is
	// called again
	availableCount := p.availRows.RowCount()
	out := p.rowsFactory.NewRows(limit)

	var newOutRows *common.Rows
	for i := 0; i < availableCount; i++ {
		row := p.availRows.GetRow(i)
		if i < limit {
			out.AppendRow(row)
		} else {
			if newOutRows == nil {
				newOutRows = p.rowsFactory.NewRows(availableCount - limit)
			}
			newOutRows.AppendRow(row)
		}
	}
	p.availRows = newOutRows

	return out, nil
}
