package exec

import (
	"fmt"
	"github.com/squareup/pranadb/common"
)

type ExecutorType int

const (
	ExecutorTypeSelect ExecutorType = iota
	ExecutorTypeAggregation
	ExecutorTypeTableScan
)

func CreateExecutor(executorType ExecutorType) (PullExecutor, error) {
	switch executorType {
	case ExecutorTypeSelect:
		return &PullSelect{}, nil
	case ExecutorTypeTableScan:
		return &PullTableScan{}, nil
	default:
		return nil, fmt.Errorf("unexpected executorType %d", executorType)
	}
}

type PullExecutor interface {
	GetRows(limit int) (rows *common.Rows, err error)
	SetParent(parent PullExecutor)
	AddChild(child PullExecutor)
	GetParent() PullExecutor
	GetChildren() []PullExecutor
}

type pullExecutorBase struct {
	colNames    []string
	colTypes    []common.ColumnType
	keyCols     []int
	rowsFactory *common.RowsFactory
	parent      PullExecutor
	children    []PullExecutor
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

func ConnectPullExecutors(childExecutors []PullExecutor, parent PullExecutor) {
	for _, child := range childExecutors {
		child.SetParent(parent)
		parent.AddChild(child)
	}
}
