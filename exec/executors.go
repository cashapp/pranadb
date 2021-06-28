package exec

import (
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/storage"
)

type PullExecutor interface {
	GetRows(limit int) (rows *common.Rows, err error)
	SetParent(parent PullExecutor)
	AddChild(parent PullExecutor)
	GetParent() PullExecutor
	GetChildren() []PullExecutor
}

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
	WriteBatch *storage.WriteBatch
	Forwarder  Forwarder
}

type Forwarder interface {
	QueueForRemoteSend(key []byte, remoteShardID uint64, row *common.Row, localShardID uint64,
		entityID uint64, colTypes []common.ColumnType, batch *storage.WriteBatch) error
}

type executorBase struct {
	colNames    []string
	colTypes    []common.ColumnType
	keyCols     []int
	rowsFactory *common.RowsFactory
}

type pullExecutorBase struct {
	executorBase
	parent   PullExecutor
	children []PullExecutor
}

type pushExecutorBase struct {
	executorBase
	parent   PushExecutor
	children []PushExecutor
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

func (p *executorBase) KeyCols() []int {
	return p.keyCols
}

func (p *executorBase) ColNames() []string {
	return p.colNames
}

func (p *executorBase) ColTypes() []common.ColumnType {
	return p.colTypes
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

func ConnectPushExecutors(childExecutors []PushExecutor, parent PushExecutor) {
	for _, child := range childExecutors {
		child.SetParent(parent)
		parent.AddChild(child)
	}
}

func ConnectPullExecutors(childExecutors []PullExecutor, parent PullExecutor) {
	for _, child := range childExecutors {
		child.SetParent(parent)
		parent.AddChild(child)
	}
}
