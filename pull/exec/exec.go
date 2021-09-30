package exec

import (
	"github.com/pkg/errors"
	"github.com/squareup/pranadb/perrors"
	"time"

	"github.com/squareup/pranadb/common"
)

type ExecutorType uint32

const (
	ExecutorTypeSelect ExecutorType = iota
	ExecutorTypeAggregation
	ExecutorTypeTableScan
)

const (
	OrderByMaxRows    = 10000
	loadRowsBatchSize = 100
	loadRowsTimeout   = time.Second * 5
)

func CreateExecutor(executorType ExecutorType) (PullExecutor, error) {
	switch executorType {
	case ExecutorTypeSelect:
		return &PullSelect{}, nil
	case ExecutorTypeTableScan:
		return &PullTableScan{}, nil
	case ExecutorTypeAggregation:
		return nil, perrors.Errorf("not implemented")
	default:
		return nil, perrors.Errorf("unexpected executorType %d", executorType)
	}
}

type PullExecutor interface {
	GetRows(limit int) (rows *common.Rows, err error)
	SetParent(parent PullExecutor)
	AddChild(child PullExecutor)
	GetParent() PullExecutor
	GetChildren() []PullExecutor
	ColNames() []string
	SimpleColNames() []string
	ColTypes() []common.ColumnType
	Reset()
}

type pullExecutorBase struct {
	colNames       []string
	simpleColNames []string
	colTypes       []common.ColumnType
	keyCols        []int
	rowsFactory    *common.RowsFactory
	parent         PullExecutor
	children       []PullExecutor
}

func (p *pullExecutorBase) SetParent(parent PullExecutor) {
	p.parent = parent
}

func (p *pullExecutorBase) AddChild(child PullExecutor) {
	p.children = append(p.children, child)
}

func (p *pullExecutorBase) GetParent() PullExecutor {
	return p.parent
}

func (p *pullExecutorBase) GetChildren() []PullExecutor {
	return p.children
}

func (p *pullExecutorBase) KeyCols() []int {
	return p.keyCols
}

func (p *pullExecutorBase) ColNames() []string {
	return p.colNames
}

func (p *pullExecutorBase) ColTypes() []common.ColumnType {
	return p.colTypes
}

func (p *pullExecutorBase) SimpleColNames() []string {
	return p.simpleColNames
}

func (p *pullExecutorBase) Reset() {
	for _, child := range p.children {
		child.Reset()
	}
}

func ConnectPullExecutors(childExecutors []PullExecutor, parent PullExecutor) {
	for _, child := range childExecutors {
		child.SetParent(parent)
		parent.AddChild(child)
	}
}

// LoadAllInBatches loads all the rows from the query - only use this if you expect the number of rows to be
// reasonably small. It will timeout.
func LoadAllInBatches(exec PullExecutor) (*common.Rows, error) {
	start := time.Now()
	var rows *common.Rows
	for {
		r, err := exec.GetRows(loadRowsBatchSize)
		if err != nil {
			return nil, err
		}
		if rows == nil {
			rows = r
		} else {
			rows.AppendAll(r)
		}
		if r.RowCount() < loadRowsBatchSize {
			break
		}
		if time.Now().Sub(start) >= loadRowsTimeout {
			return nil, errors.New("timed out in loading all rows")
		}
	}
	return rows, nil
}
