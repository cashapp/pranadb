package command

import (
	"fmt"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/meta"
	"strings"
	"sync"
)

type DropMVCommand struct {
	lock       sync.Mutex
	e          *Executor
	schemaName string
	sql        string
	mvInfo     *common.MaterializedViewInfo
	schema     *common.Schema
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

	mvInfo, err := c.getMVInfo()
	if err != nil {
		return err
	}
	c.mvInfo = mvInfo

	// Update row in tables table to mark it as pending delete
	return c.e.metaController.PersistMaterializedView(mvInfo, meta.PrepareStateDelete)
}

func (c *DropMVCommand) OnPrepare() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	// MV should be removed from in memory metadata and disconnected from it's sources and
	// any aggregate tables disconnected as remote receivers
	if c.mvInfo == nil {
		mvInfo, err := c.getMVInfo()
		if err != nil {
			return err
		}
		c.mvInfo = mvInfo
	}
	if err := c.e.metaController.UnregisterMaterializedview(c.schemaName, c.mvInfo.Name); err != nil {
		return err
	}
	schema, ok := c.e.metaController.GetSchema(c.schemaName)
	if !ok {
		return fmt.Errorf("no such schema %s", c.schemaName)
	}
	c.schema = schema
	return c.e.pushEngine.DisconnectMV(schema, c.mvInfo)
}

func (c *DropMVCommand) OnCommit() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	// Remove the mv from the push engine and delete all it's data
	// Deleting the data could take some time
	return c.e.pushEngine.RemoveMV(c.schema, c.mvInfo)
}

func (c *DropMVCommand) AfterCommit() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	// Delete the mv from the tables table
	return c.e.metaController.DeleteMaterializedView(c.mvInfo.ID)
}

func (c *DropMVCommand) getMVInfo() (*common.MaterializedViewInfo, error) {
	// TODO we should really use the parser to do this
	parts := strings.Split(c.sql, " ")
	if len(parts) < 4 {
		return nil, errors.MaybeAddStack(fmt.Errorf("invalid drop statement %s", c.sql))
	}
	mvName := parts[3]
	mvInfo, ok := c.e.metaController.GetMaterializedView(c.schemaName, mvName)
	if !ok {
		return nil, errors.MaybeAddStack(fmt.Errorf("unknown mv %s", mvName))
	}
	return mvInfo, nil
}
