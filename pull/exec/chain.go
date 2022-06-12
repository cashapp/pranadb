package exec

import (
	"github.com/squareup/pranadb/common"
)

// PullChain combines a sequence of executors in a linked list and exposes them as a single executor.
type PullChain struct {
	nonEmptyChain []PullExecutor
	colNames      []string
}

var _ PullExecutor = (*PullChain)(nil)

func NewPullChain(head PullExecutor, tail ...PullExecutor) *PullChain {
	chain := make([]PullExecutor, 1, len(tail)+1)
	chain[0] = head
	chain = append(chain, tail...)
	result := &PullChain{nonEmptyChain: chain}
	result.connectExecutors()
	return result
}

func (c *PullChain) connectExecutors() {
	for i := 0; i < len(c.nonEmptyChain)-1; i++ {
		ConnectPullExecutors([]PullExecutor{c.nonEmptyChain[i+1]}, c.nonEmptyChain[i])
	}
}

func (c *PullChain) first() PullExecutor {
	return c.nonEmptyChain[0]
}

func (c *PullChain) last() PullExecutor {
	return c.nonEmptyChain[len(c.nonEmptyChain)-1]
}

func (c *PullChain) GetRows(limit int) (*common.Rows, error) {
	return c.first().GetRows(limit)
}

func (c *PullChain) AddChild(child PullExecutor) {
	c.last().AddChild(child)
}

func (c *PullChain) GetChildren() []PullExecutor {
	return c.last().GetChildren()
}

func (c *PullChain) GetParent() PullExecutor {
	return c.first().GetParent()
}

func (c *PullChain) SetParent(parent PullExecutor) {
	c.first().SetParent(parent)
}

func (c *PullChain) ColNames() []string {
	return c.colNames
}

func (c *PullChain) ColTypes() []common.ColumnType {
	return c.first().ColTypes()
}

func (c *PullChain) SetColNames(colNames []string) {
	c.colNames = colNames
}
