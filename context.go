package pranadb

import "github.com/pingcap/tidb/sessionctx"

type ExecutionContext struct {
	TiDBContext sessionctx.Context
}

func NewExecutionContext(tiDBContext sessionctx.Context) *ExecutionContext {
	return &ExecutionContext{
		TiDBContext: tiDBContext,
	}
}
