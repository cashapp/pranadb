package exec

import (
	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/push/mover"
)

type PushExecutor interface {
	HandleRows(rows *common.Rows, ctx *ExecutionContext) error

	SetParent(parent PushExecutor)
	GetParent() PushExecutor
	AddChild(parent PushExecutor)
	GetChildren() []PushExecutor
	ClearChildren()
	ReCalcSchemaFromChildren()
	ColNames() []string
	ColTypes() []common.ColumnType
	KeyCols() []int
}

type ExecutionContext struct {
	WriteBatch *cluster.WriteBatch
	Mover      *mover.Mover
}

type pushExecutorBase struct {
	colNames    []string
	colTypes    []common.ColumnType
	keyCols     []int
	rowsFactory *common.RowsFactory
	parent      PushExecutor
	children    []PushExecutor
}

func (p *pushExecutorBase) SetParent(parent PushExecutor) {
	p.parent = parent
}

func (p *pushExecutorBase) GetParent() PushExecutor {
	return p.parent
}

func (p *pushExecutorBase) AddChild(child PushExecutor) {
	p.children = append(p.children, child)
}

func (p *pushExecutorBase) GetChildren() []PushExecutor {
	return p.children
}

func (p *pushExecutorBase) ClearChildren() {
	p.children = make([]PushExecutor, 0)
}

func (p *pushExecutorBase) KeyCols() []int {
	return p.keyCols
}

func (p *pushExecutorBase) ColNames() []string {
	return p.colNames
}

func (p *pushExecutorBase) ColTypes() []common.ColumnType {
	return p.colTypes
}

func ConnectPushExecutors(childExecutors []PushExecutor, parent PushExecutor) {
	for _, child := range childExecutors {
		child.SetParent(parent)
		parent.AddChild(child)
	}
}
