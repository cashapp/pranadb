package exec

import (
	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/push/mover"
)

type PushExecutor interface {
	HandleRows(rowsBatch RowsBatch, ctx *ExecutionContext) error

	SetParent(parent PushExecutor)
	GetParent() PushExecutor
	AddChild(parent PushExecutor)
	GetChildren() []PushExecutor
	ClearChildren()
	ReCalcSchemaFromChildren() error
	ColNames() []string
	SetColNames(colNames []string)
	ColTypes() []common.ColumnType
	KeyCols() []int
	ColsVisible() []bool
}

type ExecutionContext struct {
	WriteBatch *cluster.WriteBatch
	Mover      *mover.Mover
}

type pushExecutorBase struct {
	colNames []string
	colTypes []common.ColumnType
	// determines whether the column at index i is visible to the user or not
	// by default this array is nil which means all columns are visible
	colsVisible []bool
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

func (p *pushExecutorBase) SetColNames(colNames []string) {
	p.colNames = colNames
}

func (p *pushExecutorBase) ColTypes() []common.ColumnType {
	return p.colTypes
}

func (p *pushExecutorBase) ColsVisible() []bool {
	return p.colsVisible
}

func (p *pushExecutorBase) ReCalcSchemaFromChildren() error {
	return nil
}

func ConnectPushExecutors(childExecutors []PushExecutor, parent PushExecutor) {
	for _, child := range childExecutors {
		child.SetParent(parent)
		parent.AddChild(child)
	}
}
