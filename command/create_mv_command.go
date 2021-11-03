package command

import (
	"fmt"
	"sync"

	"github.com/squareup/pranadb/command/parser"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/meta"
	"github.com/squareup/pranadb/parplan"
	"github.com/squareup/pranadb/push"
)

type CreateMVCommand struct {
	lock           sync.Mutex
	e              *Executor
	pl             *parplan.Planner
	schema         *common.Schema
	createMVSQL    string
	tableSequences []uint64
	mv             *push.MaterializedView
	ast            *parser.CreateMaterializedView
}

func (c *CreateMVCommand) CommandType() DDLCommandType {
	return DDLCommandTypeCreateMV
}

func (c *CreateMVCommand) SchemaName() string {
	return c.schema.Name
}

func (c *CreateMVCommand) SQL() string {
	return c.createMVSQL
}

func (c *CreateMVCommand) TableSequences() []uint64 {
	return c.tableSequences
}

func (c *CreateMVCommand) LockName() string {
	return c.schema.Name + "/"
}

func NewOriginatingCreateMVCommand(e *Executor, pl *parplan.Planner, schema *common.Schema, sql string, tableSequences []uint64, ast *parser.CreateMaterializedView) *CreateMVCommand {
	pl.RefreshInfoSchema()
	return &CreateMVCommand{
		e:              e,
		schema:         schema,
		pl:             pl,
		ast:            ast,
		createMVSQL:    sql,
		tableSequences: tableSequences,
	}
}

func NewCreateMVCommand(e *Executor, schemaName string, createMVSQL string, tableSequences []uint64) *CreateMVCommand {
	schema := e.metaController.GetOrCreateSchema(schemaName)
	pl := parplan.NewPlanner(schema, false)
	return &CreateMVCommand{
		e:              e,
		schema:         schema,
		pl:             pl,
		createMVSQL:    createMVSQL,
		tableSequences: tableSequences,
	}
}

func (c *CreateMVCommand) BeforePrepare() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	// Before prepare we just persist the source info in the tables table
	mv, err := c.createMVFromAST(c.ast)
	if err != nil {
		return errors.WithStack(err)
	}
	c.mv = mv

	_, ok := c.e.metaController.GetMaterializedView(mv.Info.SchemaName, mv.Info.Name)
	if ok {
		return errors.NewMaterializedViewAlreadyExistsError(mv.Info.SchemaName, mv.Info.Name)
	}
	rows, err := c.e.pullEngine.ExecuteQuery("sys",
		fmt.Sprintf("select id from tables where schema_name='%s' and name='%s' and kind='%s'", c.mv.Info.SchemaName, c.mv.Info.Name, meta.TableKindMaterializedView))
	if err != nil {
		return errors.WithStack(err)
	}
	if rows.RowCount() != 0 {
		return errors.Errorf("source with name %s.%s already exists in storage", c.mv.Info.SchemaName, c.mv.Info.Name)
	}
	return c.e.metaController.PersistMaterializedView(mv.Info, mv.InternalTables, meta.PrepareStateAdd)
}

func (c *CreateMVCommand) OnPrepare() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	// If receiving on prepare from broadcast on the originating node, mv will already be set
	// this means we do not have to parse the ast twice!
	if c.mv == nil {
		mv, err := c.createMV()
		if err != nil {
			return errors.WithStack(err)
		}
		c.mv = mv
	}

	// We must first connect any aggregations in the MV as remote consumers as they might have rows forwarded to them
	// during the MV fill process. This must be done on all nodes before we start the fill
	// We do not join the MV up to it's feeding sources or MVs at this point
	return c.mv.Connect(false, true)
}

func (c *CreateMVCommand) OnCommit() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	// Fill the MV from it's feeding sources and MVs
	// Before the fill completes the MV will be connected to it's feeding sources or MVs
	if err := c.mv.Fill(); err != nil {
		return errors.WithStack(err)
	}
	if err := c.e.pushEngine.RegisterMV(c.mv); err != nil {
		return errors.WithStack(err)
	}
	return c.e.metaController.RegisterMaterializedView(c.mv.Info, c.mv.InternalTables)
}

func (c *CreateMVCommand) AfterCommit() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.e.metaController.PersistMaterializedView(c.mv.Info, c.mv.InternalTables, meta.PrepareStateAdd)
}

func (c *CreateMVCommand) createMVFromAST(ast *parser.CreateMaterializedView) (*push.MaterializedView, error) {
	mvName := ast.Name.String()
	querySQL := ast.Query.String()
	seqGenerator := common.NewPreallocSeqGen(c.tableSequences)
	tableID := seqGenerator.GenerateSequence()
	mv, err := push.CreateMaterializedView(c.e.pushEngine, c.pl, c.schema, mvName, querySQL, tableID, seqGenerator)
	return mv, errors.WithStack(err)
}

func (c *CreateMVCommand) createMV() (*push.MaterializedView, error) {
	ast, err := parser.Parse(c.createMVSQL)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if ast.Create == nil || ast.Create.MaterializedView == nil {
		return nil, errors.Errorf("not a create materialized view %s", c.createMVSQL)
	}
	return c.createMVFromAST(ast.Create.MaterializedView)
}
