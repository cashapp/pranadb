package exec

import (
	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/push/sched"
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

func NewExecutionContext(writeBatch *cluster.WriteBatch, rowCache sched.RowCache, fillTableID int64) *ExecutionContext {
	return &ExecutionContext{
		WriteBatch:  writeBatch,
		FillTableID: fillTableID,
		RowCache:    rowCache,
	}
}

type ExecutionContext struct {
	WriteBatch    *cluster.WriteBatch
	RemoteBatches map[uint64]*cluster.WriteBatch
	FillTableID   int64
	RowCache      sched.RowCache
}

func (e *ExecutionContext) AddToForwardBatch(shardID uint64, key []byte, value []byte) {
	if e.RemoteBatches == nil {
		e.RemoteBatches = make(map[uint64]*cluster.WriteBatch)
	}
	remoteBatch, ok := e.RemoteBatches[shardID]
	if !ok {
		remoteBatch = cluster.NewWriteBatch(shardID)
		e.RemoteBatches[shardID] = remoteBatch
	}
	remoteBatch.AddPut(key, value)
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
