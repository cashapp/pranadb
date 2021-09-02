package parplan

import (
	"context"
	"math"

	"github.com/squareup/pranadb/sessctx"

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
	ctx                *sessctx.SessCtx
	schema             *common.Schema
	is                 infoschema.InfoSchema
	pullQuery          bool
}

func NewPlanner(schema *common.Schema, pullQuery bool) *Planner {
	is := schemaToInfoSchema(schema)
	sessCtx := sessctx.NewSessionContext(is, pullQuery, schema.Name)
	// TODO different rules for push and pull queries
	pl := &Planner{
		pushQueryOptimizer: cascades.NewOptimizer(),
		pullQueryOptimizer: cascades.NewOptimizer(),
		parser:             newParser(),
		ctx:                sessCtx,
		schema:             schema,
		is:                 is,
		pullQuery:          pullQuery,
	}
	return pl
}

func (p *Planner) SetPSArgs(args []interface{}) {
	p.ctx.SetArgs(args)
}

func (p *Planner) RefreshInfoSchema() {
	p.is = schemaToInfoSchema(p.schema)
	p.ctx.SetInfoSchema(p.is)
}

func (p *Planner) Parse(query string) (AstHandle, error) {
	return p.parser.Parse(query)
}

func (p *Planner) QueryToPlan(query string, prepare bool) (core.PhysicalPlan, core.LogicalPlan, error) {
	ast, err := p.Parse(query)
	if err != nil {
		return nil, nil, err
	}
	return p.BuildPhysicalPlan(ast, prepare)
}

func (p *Planner) BuildPhysicalPlan(stmt AstHandle, prepare bool) (core.PhysicalPlan, core.LogicalPlan, error) {

	var err error
	if prepare {
		err = core.Preprocess(p.ctx, stmt.stmt, core.InPrepare)
	} else {
		err = core.Preprocess(p.ctx, stmt.stmt)
	}
	if err != nil {
		return nil, nil, err
	}
	logicalPlan, err := p.createLogicalPlan(context.TODO(), p.ctx, stmt.stmt, p.is)
	if err != nil {
		return nil, nil, err
	}

	phys, err := p.createPhysicalPlan(context.TODO(), p.ctx, logicalPlan, true, true)
	if err != nil {
		return nil, nil, err
	}
	return phys, logicalPlan, nil
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
