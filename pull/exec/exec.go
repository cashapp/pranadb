package exec

import (
	"github.com/squareup/pranadb/common"
)

type ExecutorType uint32

const (
	orderByMaxRows = 50000
	queryBatchSize = 10000
)

type PullExecutor interface {
	/*
		GetRows returns up to a maximum of limit rows. If less than limit rows are returned that means there are no more
		rows to return. If limit rows are returned it means there may be more rows to return and the caller should call
		GetRows again until less than limit rows are returned.
	*/
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
