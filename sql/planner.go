package sql

import (
	"context"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/planner/cascades"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/hint"
)

type Planner interface {
	CreateLogicalPlan(ctx context.Context, sessionContext sessionctx.Context, node ast.Node, is infoschema.InfoSchema) (core.LogicalPlan, error)
	CreatePhysicalPlan(ctx context.Context, sessionContext sessionctx.Context, logicalPlan core.LogicalPlan, isPushQuery, useCascades bool) (core.PhysicalPlan, error)
}

type planner struct {
	pushQueryOptimizer *cascades.Optimizer
	pullQueryOptimizer *cascades.Optimizer
	parser             Parser
}

func NewPlanner() Planner {
	// TODO different rules for push and pull queries
	return &planner{
		pushQueryOptimizer: cascades.NewOptimizer(),
		pullQueryOptimizer: cascades.NewOptimizer(),
		parser:             NewParser(),
	}
}

func (p *planner) CreateLogicalPlan(ctx context.Context, sessionContext sessionctx.Context, node ast.Node, is infoschema.InfoSchema) (core.LogicalPlan, error) {

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

func (p *planner) CreatePhysicalPlan(ctx context.Context, sessionContext sessionctx.Context, logicalPlan core.LogicalPlan, isPushQuery, useCascades bool) (core.PhysicalPlan, error) {
	if useCascades {
		// Use the new cost based optimizer
		if isPushQuery {
			physicalPlan, _, err := p.pushQueryOptimizer.FindBestPlan(sessionContext, logicalPlan)
			if err != nil {
				return nil, err
			}
			return physicalPlan, nil
		} else {
			physicalPlan, _, err := p.pullQueryOptimizer.FindBestPlan(sessionContext, logicalPlan)
			if err != nil {
				return nil, err
			}
			return physicalPlan, nil
		}
	} else {
		// Use the older optimizer
		physicalPlan, _, err := core.DoOptimize(ctx, sessionContext, 0, logicalPlan)
		if err != nil {
			return nil, err
		}
		return physicalPlan, nil
	}
}
