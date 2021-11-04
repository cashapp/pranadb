package command

import (
	"sync"

	"github.com/squareup/pranadb/command/parser"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/meta"
)

type DropIndexCommand struct {
	lock        sync.Mutex
	e           *Executor
	schemaName  string
	sql         string
	tableName   string
	indexName   string
	indexInfo   *common.IndexInfo
	originating bool
}

func (c *DropIndexCommand) CommandType() DDLCommandType {
	return DDLCommandTypeDropIndex
}

func (c *DropIndexCommand) SchemaName() string {
	return c.schemaName
}

func (c *DropIndexCommand) SQL() string {
	return c.sql
}

func (c *DropIndexCommand) TableSequences() []uint64 {
	return nil
}

func (c *DropIndexCommand) LockName() string {
	return c.schemaName + "/"
}

func NewOriginatingDropIndexCommand(e *Executor, schemaName string, sql string, tableName string, indexName string) *DropIndexCommand {
	return &DropIndexCommand{
		e:           e,
		schemaName:  schemaName,
		sql:         sql,
		tableName:   tableName,
		indexName:   indexName,
		originating: true,
	}
}

func NewDropIndexCommand(e *Executor, schemaName string, sql string) *DropIndexCommand {
	return &DropIndexCommand{
		e:          e,
		schemaName: schemaName,
		sql:        sql,
	}
}

func (c *DropIndexCommand) BeforePrepare() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	indexInfo, err := c.getIndexInfo()
	if err != nil {
		return errors.WithStack(err)
	}
	c.indexInfo = indexInfo

	// Update row in indexes table to mark it as pending delete
	return c.e.metaController.PersistIndex(indexInfo, meta.PrepareStateDelete)
}

func (c *DropIndexCommand) OnPrepare() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.indexInfo == nil {
		indexInfo, err := c.getIndexInfo()
		if err != nil {
			return errors.WithStack(err)
		}
		c.indexInfo = indexInfo
	}
	if err := c.e.metaController.UnregisterIndex(c.schemaName, c.indexInfo.TableName, c.indexInfo.Name); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (c *DropIndexCommand) OnCommit() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	return c.e.pushEngine.RemoveIndex(c.indexInfo, c.originating)
}

func (c *DropIndexCommand) AfterCommit() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	// Delete the index info from the indexes table
	return c.e.metaController.DeleteIndex(c.indexInfo.ID)
}

func (c *DropIndexCommand) getIndexInfo() (*common.IndexInfo, error) {
	if c.indexName == "" {
		ast, err := parser.Parse(c.sql)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		if ast.Drop == nil && !ast.Drop.Index {
			return nil, errors.Errorf("not a drop index command %s", c.sql)
		}
		c.indexName = ast.Drop.Name
		c.tableName = ast.Drop.TableName
	}
	indexInfo, ok := c.e.metaController.GetIndex(c.schemaName, c.tableName, c.indexName)
	if !ok {
		return nil, errors.NewUnknownIndexError(c.schemaName, c.tableName, c.indexName)
	}
	return indexInfo, nil
}
