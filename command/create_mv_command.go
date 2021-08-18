package command

import (
	"fmt"
	"github.com/squareup/pranadb/command/parser"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/meta"
	"github.com/squareup/pranadb/parplan"
	"github.com/squareup/pranadb/push"
	"sync"
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
	return &CreateMVCommand{
		e:              e,
		schema:         schema,
		pl:             pl,
		ast:            ast,
		createMVSQL:    sql,
		tableSequences: tableSequences,
	}
}

func NewCreateMVCommand(e *Executor, pl *parplan.Planner, schema *common.Schema, createMVSQL string, tableSequences []uint64) *CreateMVCommand {
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
		return err
	}
	c.mv = mv

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
			return err
		}
		c.mv = mv
	}

	// TODO Build the MV state from it's sources

	return nil
}

func (c *CreateMVCommand) OnCommit() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if err := c.mv.Connect(); err != nil {
		return err
	}
	if err := c.e.pushEngine.RegisterMV(c.mv); err != nil {
		return err
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
	return mv, err
}

func (c *CreateMVCommand) createMV() (*push.MaterializedView, error) {
	ast, err := parser.Parse(c.createMVSQL)
	if err != nil {
		return nil, errors.MaybeAddStack(err)
	}
	if ast.Create == nil || ast.Create.MaterializedView == nil {
		return nil, fmt.Errorf("not a create materialized view %s", c.createMVSQL)
	}
	return c.createMVFromAST(ast.Create.MaterializedView)
}
