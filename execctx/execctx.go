package execctx

import (
	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/parplan"
)

type ExecutionContext struct {
	ID           string
	Schema       *common.Schema
	planner      *parplan.Planner
	QueryInfo    *cluster.QueryExecutionInfo
	CurrentQuery interface{} // typed as interface{} to avoid circular dependency with pull
}

func NewExecutionContext(id string, schema *common.Schema) *ExecutionContext {
	return &ExecutionContext{
		ID:        id,
		QueryInfo: new(cluster.QueryExecutionInfo),
		Schema:    schema,
	}
}

func (s *ExecutionContext) Planner() *parplan.Planner {
	if s.planner == nil {
		s.planner = parplan.NewPlanner(s.Schema)
	}
	return s.planner
}
