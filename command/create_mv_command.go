package command

import (
	"fmt"
	"github.com/squareup/pranadb/command/parser"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/meta"
	"sync"
)

type CreateMVCommand struct {
	lock           sync.Mutex
	e              *Executor
	schemaName     string
	sql            string
	tableSequences []uint64
	ast            *parser.CreateMaterializedView
	mvInfo         *common.MaterializedViewInfo
}

func (c *CreateMVCommand) CommandType() DDLCommandType {
	return DDLCommandTypeCreateSource
}

func (c *CreateMVCommand) SchemaName() string {
	return c.schemaName
}

func (c *CreateMVCommand) SQL() string {
	return c.sql
}

func (c *CreateMVCommand) TableSequences() []uint64 {
	return c.tableSequences
}

func (c *CreateMVCommand) LockName() string {
	return c.schemaName + "/"
}

func NewOriginatingCreateMVCommand(e *Executor, schemaName string, sql string, tableSequences []uint64, ast *parser.CreateMaterializedView) *CreateMVCommand {
	return &CreateMVCommand{
		e:              e,
		schemaName:     schemaName,
		sql:            sql,
		tableSequences: tableSequences,
		ast:            ast,
	}
}

func NewCreateMVCommand(e *Executor, schemaName string, sql string, tableSequences []uint64) *CreateMVCommand {
	return &CreateMVCommand{
		e:              e,
		schemaName:     schemaName,
		sql:            sql,
		tableSequences: tableSequences,
	}
}

func (c *CreateMVCommand) BeforePrepare() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	// Before prepare we just persist the source info in the tables table
	var err error
	c.mvInfo, err = c.getMVInfo(c.ast)
	if err != nil {
		return err
	}
	return c.e.metaController.PersistMaterializedView(c.mvInfo, meta.PrepareStateAdd)
}

func (c *CreateMVCommand) OnPrepare() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	// If receiving on prepare from broadcast on the originating node, mvInfo will already be set
	// this means we do not have to parse the ast twice!
	if c.mvInfo == nil {
		ast, err := parser.Parse(c.sql)
		if err != nil {
			return errors.MaybeAddStack(err)
		}
		if ast.Create == nil || ast.Create.MaterializedView == nil {
			return fmt.Errorf("not a create materialized view %s", c.sql)
		}
		c.mvInfo, err = c.getMVInfo(ast.Create.MaterializedView)
		if err != nil {
			return err
		}
	}

	// TODO Build the MV state from it's sources

	//c.e.pushEngine.CreateMaterializedView()
	//
	//return c.e.pushEngine.CreateSource(c.sourceInfo)
	return nil
}

func (c *CreateMVCommand) OnCommit() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	// TODO connect the MV to it's sources

	// Register the source in the in memory meta data
	//return c.e.metaController.RegisterMaterializedView(c.mvInfo, meta.PrepareStateCommitted)
	return nil
}

func (c *CreateMVCommand) AfterCommit() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	return c.e.metaController.PersistMaterializedView(c.mvInfo, meta.PrepareStateCommitted)
}

func (c *CreateMVCommand) getMVInfo(ast *parser.CreateMaterializedView) (*common.MaterializedViewInfo, error) {
	return nil, nil
}
