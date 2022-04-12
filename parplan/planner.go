package parplan

import (
	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/tidb/planner"
	"github.com/squareup/pranadb/tidb/sessionctx/stmtctx"

	"github.com/squareup/pranadb/sessctx"

	"github.com/pingcap/parser/ast"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/tidb/infoschema"
)

type Planner struct {
	pushQueryOptimizer *planner.Optimizer
	pullQueryOptimizer *planner.Optimizer
	parser             *Parser
	sessionCtx         *sessctx.SessCtx
	schema             *common.Schema
	is                 infoschema.InfoSchema
}

func NewPlanner(schema *common.Schema) *Planner {
	is := schemaToInfoSchema(schema)
	sessCtx := sessctx.NewSessionContext(is, schema.Name)
	// TODO different rules for push and pull queries
	pl := &Planner{
		pushQueryOptimizer: planner.NewPushQueryOptimizer(),
		pullQueryOptimizer: planner.NewPullQueryOptimizer(),
		parser:             NewParser(),
		sessionCtx:         sessCtx,
		schema:             schema,
		is:                 is,
	}
	return pl
}

func (p *Planner) StatementContext() *stmtctx.StatementContext {
	return p.sessionCtx.GetSessionVars().StmtCtx
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

func (p *Planner) QueryToPlan(query string, prepare bool, pullQuery bool) (planner.PhysicalPlan, planner.LogicalPlan, error) {
	ast, err := p.Parse(query)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	logic, err := p.BuildLogicalPlan(ast, prepare)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	physical, err := p.BuildPhysicalPlan(logic, pullQuery)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	return physical, logic, nil
}

func (p *Planner) BuildLogicalPlan(stmt AstHandle, prepare bool) (planner.LogicalPlan, error) {
	if err := p.preprocess(stmt.stmt, prepare); err != nil {
		return nil, err
	}
	builder := planner.NewPlanBuilder(p.sessionCtx, p.is)
	plan, err := builder.Build(stmt.stmt)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	logicalPlan, isLogicalPlan := plan.(planner.LogicalPlan)
	if !isLogicalPlan {
		panic("Expected a logical plan")
	}
	return logicalPlan, nil
}

func (p *Planner) BuildPhysicalPlan(logicalPlan planner.LogicalPlan, pullQuery bool) (planner.PhysicalPlan, error) {
	if !pullQuery {
		physicalPlan, _, err := p.pushQueryOptimizer.FindBestPlan(p.sessionCtx, logicalPlan)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return physicalPlan, nil
	}
	physicalPlan, _, err := p.pullQueryOptimizer.FindBestPlan(p.sessionCtx, logicalPlan)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return physicalPlan, nil
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
