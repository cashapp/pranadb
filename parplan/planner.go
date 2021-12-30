package parplan

import (
	"context"
	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/tidb/planner"

	"github.com/squareup/pranadb/sessctx"

	"github.com/pingcap/parser/ast"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/tidb/infoschema"
	"github.com/squareup/pranadb/tidb/sessionctx"
)

type Planner struct {
	pushQueryOptimizer *planner.Optimizer
	pullQueryOptimizer *planner.Optimizer
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
		pushQueryOptimizer: planner.NewOptimizer(),
		pullQueryOptimizer: planner.NewOptimizer(),
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

func (p *Planner) QueryToPlan(query string, prepare bool) (planner.PhysicalPlan, planner.LogicalPlan, error) {
	ast, err := p.Parse(query)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	return p.BuildPhysicalPlan(ast, prepare)
}

func (p *Planner) BuildPhysicalPlan(stmt AstHandle, prepare bool) (planner.PhysicalPlan, planner.LogicalPlan, error) {

	var err error
	if prepare {
		err = planner.Preprocess(p.ctx, stmt.stmt, planner.InPrepare)
	} else {
		err = planner.Preprocess(p.ctx, stmt.stmt)
	}
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	logicalPlan, err := p.createLogicalPlan(context.TODO(), p.ctx, stmt.stmt, p.is)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}

	phys, err := p.createPhysicalPlan(p.ctx, logicalPlan, true)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	return phys, logicalPlan, nil
}

func (p *Planner) createLogicalPlan(ctx context.Context, sessionContext sessionctx.Context, node ast.Node, is infoschema.InfoSchema) (planner.LogicalPlan, error) {
	builder := planner.NewPlanBuilder(sessionContext, is)
	plan, err := builder.Build(ctx, node)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	logicalPlan, isLogicalPlan := plan.(planner.LogicalPlan)
	if !isLogicalPlan {
		panic("Expected a logical plan")
	}
	return logicalPlan, nil
}

func (p *Planner) createPhysicalPlan(sessionContext sessionctx.Context, logicalPlan planner.LogicalPlan, isPushQuery bool) (planner.PhysicalPlan, error) {
	// Use the new cost based optimizer
	if isPushQuery {
		physicalPlan, _, err := p.pushQueryOptimizer.FindBestPlan(sessionContext, logicalPlan)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return physicalPlan, nil
	}
	physicalPlan, _, err := p.pullQueryOptimizer.FindBestPlan(sessionContext, logicalPlan)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return physicalPlan, nil
}
