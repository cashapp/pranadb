package parplan

import (
	"context"
	"github.com/squareup/pranadb/sessctx"
	"math"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/planner/cascades"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/hint"
	"github.com/squareup/pranadb/common"
)

type Planner struct {
	pushQueryOptimizer *cascades.Optimizer
	pullQueryOptimizer *cascades.Optimizer
	parser             *parser
}

func NewPlanner() *Planner {
	// TODO different rules for push and pull queries
	return &Planner{
		pushQueryOptimizer: cascades.NewOptimizer(),
		pullQueryOptimizer: cascades.NewOptimizer(),
		parser:             newParser(),
	}
}

func (p *Planner) QueryToPlan(schema *common.Schema, query string, pullQuery bool) (core.PhysicalPlan, error) {
	stmt, err := p.parser.Parse(query)
	if err != nil {
		return nil, err
	}
	is, err := schemaToInfoSchema(schema)
	if err != nil {
		return nil, err
	}
	ctx := context.TODO()
	sessCtx := sessctx.NewSessionContext(is, pullQuery)
	//TODO doesn't seem to work if there are prepared statement markers
	err = core.Preprocess(sessCtx, stmt)
	if err != nil {
		return nil, err
	}
	logicalPlan, err := p.createLogicalPlan(ctx, sessCtx, stmt, is)
	if err != nil {
		return nil, err
	}
	return p.createPhysicalPlan(ctx, sessCtx, logicalPlan, true, true)
}

func (p *Planner) createLogicalPlan(ctx context.Context, sessionContext sessionctx.Context, node ast.Node, is infoschema.InfoSchema) (core.LogicalPlan, error) {
	hintProcessor := &hint.BlockHintProcessor{Ctx: sessionContext}
	builder, _ := core.NewPlanBuilder(sessionContext, is, hintProcessor)
	plan, err := builder.Build(ctx, node)
	if err != nil {
		return nil, err
	}
	logicalPlan, isLogicalPlan := plan.(core.LogicalPlan)
	if !isLogicalPlan {
		panic("Expected a logical plan")
	}
	return logicalPlan, nil
}

func (p *Planner) createPhysicalPlan(ctx context.Context, sessionContext sessionctx.Context, logicalPlan core.LogicalPlan, isPushQuery, useCascades bool) (core.PhysicalPlan, error) {
	if useCascades {
		// Use the new cost based optimizer
		if isPushQuery {
			physicalPlan, _, err := p.pushQueryOptimizer.FindBestPlan(sessionContext, logicalPlan)
			if err != nil {
				return nil, err
			}
			return physicalPlan, nil
		}
		physicalPlan, _, err := p.pullQueryOptimizer.FindBestPlan(sessionContext, logicalPlan)
		if err != nil {
			return nil, err
		}
		return physicalPlan, nil
	}
	// Use the older optimizer
	flag := uint64(math.MaxUint64)
	physicalPlan, _, err := core.DoOptimize(ctx, sessionContext, flag, logicalPlan)
	if err != nil {
		return nil, err
	}
	return physicalPlan, nil
}
