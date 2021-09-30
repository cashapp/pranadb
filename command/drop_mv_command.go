package command

import (
	"github.com/squareup/pranadb/command/parser"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/meta"
	"github.com/squareup/pranadb/perrors"
	"github.com/squareup/pranadb/push"
	"sync"
)

type DropMVCommand struct {
	lock        sync.Mutex
	e           *Executor
	schemaName  string
	sql         string
	mvName      string
	mv          *push.MaterializedView
	schema      *common.Schema
	originating bool
}

func (c *DropMVCommand) CommandType() DDLCommandType {
	return DDLCommandTypeDropMV
}

func (c *DropMVCommand) SchemaName() string {
	return c.schemaName
}

func (c *DropMVCommand) SQL() string {
	return c.sql
}

func (c *DropMVCommand) TableSequences() []uint64 {
	return nil
}

func (c *DropMVCommand) LockName() string {
	return c.schemaName + "/"
}

func NewOriginatingDropMVCommand(e *Executor, schemaName string, sql string, mvName string) *DropMVCommand {
	return &DropMVCommand{
		e:           e,
		schemaName:  schemaName,
		sql:         sql,
		mvName:      mvName,
		originating: true,
	}
}

func NewDropMVCommand(e *Executor, schemaName string, sql string) *DropMVCommand {
	return &DropMVCommand{
		e:          e,
		schemaName: schemaName,
		sql:        sql,
	}
}

func (c *DropMVCommand) BeforePrepare() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	mv, err := c.getMV()
	if err != nil {
		return err
	}
	c.mv = mv

	consuming := c.mv.GetConsumingMVs()
	if len(consuming) != 0 {
		return perrors.NewMaterializedViewHasChildrenError(mv.Info.SchemaName, mv.Info.Name, consuming)
	}

	// Update row in tables table to mark it as pending delete
	return c.e.metaController.PersistMaterializedView(mv.Info, mv.InternalTables, meta.PrepareStateDelete)
}

func (c *DropMVCommand) OnPrepare() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	// MV should be removed from in memory metadata and disconnected from it's sources and
	// any aggregate tables disconnected as remote receivers
	if c.mv == nil {
		mv, err := c.getMV()
		if err != nil {
			return err
		}
		c.mv = mv
	}
	var itNames []string
	for _, it := range c.mv.InternalTables {
		itNames = append(itNames, it.Name)
	}
	if err := c.e.metaController.UnregisterMaterializedView(c.schemaName, c.mv.Info.Name, itNames); err != nil {
		return err
	}
	schema, ok := c.e.metaController.GetSchema(c.schemaName)
	if !ok {
		return perrors.Errorf("no such schema %s", c.schemaName)
	}
	c.schema = schema
	return c.mv.Disconnect()
}

func (c *DropMVCommand) OnCommit() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	// Remove the mv from the push engine and delete all it's data
	// Deleting the data could take some time
	if err := c.e.pushEngine.RemoveMV(c.mv.Info.ID); err != nil {
		return err
	}
	// We only delete the data from the originating node - otherwise all nodes would be deleting the same data
	return c.mv.Drop(c.originating)
}

func (c *DropMVCommand) AfterCommit() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	// Delete the mv from the tables table
	return c.e.metaController.DeleteMaterializedView(c.mv.Info, c.mv.InternalTables)
}

func (c *DropMVCommand) getMV() (*push.MaterializedView, error) {

	if c.mvName == "" {
		ast, err := parser.Parse(c.sql)
		if err != nil {
			return nil, err
		}
		if ast.Drop == nil && !ast.Drop.MaterializedView {
			return nil, perrors.Errorf("not a drop materialized view command %s", c.sql)
		}
		c.mvName = ast.Drop.Name
	}

	mvInfo, ok := c.e.metaController.GetMaterializedView(c.schemaName, c.mvName)
	if !ok {
		return nil, perrors.NewUnknownMaterializedViewError(c.schemaName, c.mvName)
	}
	mv, err := c.e.pushEngine.GetMaterializedView(mvInfo.ID)
	return mv, err
}
