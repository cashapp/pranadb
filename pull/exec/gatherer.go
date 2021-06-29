package exec

import (
	"errors"
	"github.com/squareup/pranadb/common"
)

type PullGatherer struct {
	pullExecutorBase
	asyncRowGetters []AsyncRowGetter
	queryID         string
}

type AsyncRowGetter interface {
	GetRows(limit int, queryID string) (resultChan chan AsyncRowGetterResult)
}

type AsyncRowGetterResult struct {
	Rows *common.Rows
	Err  error
}

func NewPullGatherer(colNames []string, colTypes []common.ColumnType, asyncRowGetters []AsyncRowGetter,
	queryID string) (*PullGatherer, error) {
	rf, err := common.NewRowsFactory(colTypes)
	if err != nil {
		return nil, err
	}
	base := pullExecutorBase{
		colNames:    colNames,
		colTypes:    colTypes,
		rowsFactory: rf,
	}
	return &PullGatherer{
		pullExecutorBase: base,
		asyncRowGetters:  asyncRowGetters,
		queryID:          queryID,
	}, nil
}

func (p *PullGatherer) GetRows(limit int) (rows *common.Rows, err error) {

	// TODO this can be optimised

	numGetters := len(p.asyncRowGetters)
	channels := make([]chan AsyncRowGetterResult, numGetters)
	complete := make([]bool, numGetters)
	rows = p.rowsFactory.NewRows(limit)

	completeCount := 0

	for completeCount < numGetters {
		toGet := (limit - rows.RowCount()) / (numGetters - completeCount)
		for i, getter := range p.asyncRowGetters {
			if !complete[i] {
				channels[i] = getter.GetRows(toGet, p.queryID)
			}
		}
		for i, ch := range channels {
			if !complete[i] {
				res, ok := <-ch
				if !ok {
					return nil, errors.New("channel was closed")
				}
				if res.Err != nil {
					return nil, res.Err
				}
				if res.Rows.RowCount() < toGet {
					complete[i] = true
					completeCount++
				}
				for i := 0; i < res.Rows.RowCount(); i++ {
					rows.AppendRow(res.Rows.GetRow(i))
				}
			}
		}
	}

	return rows, nil
}
