package parplan

import (
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
	parser             *Parser
	sessionCtx         *sessctx.SessCtx
	schema             *common.Schema
	is                 infoschema.InfoSchema
	pullQuery          bool
}

func NewPlanner(schema *common.Schema, pullQuery bool) *Planner {
	is := schemaToInfoSchema(schema)
	sessCtx := sessctx.NewSessionContext(is, schema.Name)
	// TODO different rules for push and pull queries
	pl := &Planner{
		pushQueryOptimizer: planner.NewOptimizer(),
		pullQueryOptimizer: planner.NewOptimizer(),
		parser:             NewParser(),
		sessionCtx:         sessCtx,
		schema:             schema,
		is:                 is,
		pullQuery:          pullQuery,
	}
	return pl
}

func (p *Planner) SetPSArgs(args []interface{}) {
	p.sessionCtx.SetArgs(args)
}

func (p *Planner) RefreshInfoSchema() {
	p.is = schemaToInfoSchema(p.schema)
	p.sessionCtx.SetInfoSchema(p.is)
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

	if err := p.preprocess(stmt.stmt, prepare); err != nil {
		return nil, nil, err
	}
	logicalPlan, err := p.createLogicalPlan(p.sessionCtx, stmt.stmt, p.is)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}

	phys, err := p.createPhysicalPlan(p.sessionCtx, logicalPlan, true)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	return phys, logicalPlan, nil
}

func (p *Planner) preprocess(stmt ast.Node, prepare bool) error {
	var err error
	if prepare {
		err = planner.Preprocess(p.sessionCtx, stmt, planner.InPrepare)
	} else {
		err = planner.Preprocess(p.sessionCtx, stmt)
	}
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (p *Planner) createLogicalPlan(sessionContext sessionctx.Context, node ast.Node, is infoschema.InfoSchema) (planner.LogicalPlan, error) {
	builder := planner.NewPlanBuilder(sessionContext, is)
	plan, err := builder.Build(node)
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
