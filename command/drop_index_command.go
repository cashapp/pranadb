package command

import (
	"github.com/squareup/pranadb/cluster"
	"strings"
	"sync"

	"github.com/squareup/pranadb/command/parser"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/errors"
)

type DropIndexCommand struct {
	lock          sync.Mutex
	e             *Executor
	schemaName    string
	sql           string
	tableName     string
	indexName     string
	indexInfo     *common.IndexInfo
	toDeleteBatch *cluster.ToDeleteBatch
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
		e:          e,
		schemaName: schemaName,
		sql:        sql,
		tableName:  strings.ToLower(tableName),
		indexName:  strings.ToLower(indexName),
	}
}

func NewDropIndexCommand(e *Executor, schemaName string, sql string) *DropIndexCommand {
	return &DropIndexCommand{
		e:          e,
		schemaName: schemaName,
		sql:        sql,
	}
}

func (c *DropIndexCommand) Before() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	indexInfo, err := c.getIndexInfo()
	if err != nil {
		return errors.WithStack(err)
	}
	c.indexInfo = indexInfo
	return nil
}

func (c *DropIndexCommand) OnPhase(phase int32) error {
	switch phase {
	case 0:
		return c.onPhase0()
	case 1:
		return c.onPhase1()
	default:
		panic("invalid phase")
	}
}

func (c *DropIndexCommand) NumPhases() int {
	return 2
}

func (c *DropIndexCommand) onPhase0() error {
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

func (c *DropIndexCommand) onPhase1() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	// This disconnects and removes the data for the index (data removal only happens on the originating node)
	return c.e.pushEngine.RemoveIndex(c.indexInfo)
}

func (c *DropIndexCommand) AfterPhase(phase int32) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	switch phase {
	case 0:
		// We record prefixes in the to_delete table - this makes sure index data is deleted on restart if failure occurs
		// after this
		var err error
		c.toDeleteBatch, err = storeToDeleteBatch(c.indexInfo.ID, c.e.cluster)
		if err != nil {
			return err
		}

		// Delete the index info from the indexes table - we must do this before we start deleting the actual index data
		// otherwise we can end up with a partial index if failure occurs
		if err := c.e.metaController.DeleteIndex(c.indexInfo.ID); err != nil {
			return err
		}
	case 1:
		// Now delete rows from the to_delete table
		return c.e.cluster.RemoveToDeleteBatch(c.toDeleteBatch)
	}
	return nil
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
		c.indexName = strings.ToLower(ast.Drop.Name)
		c.tableName = strings.ToLower(ast.Drop.TableName)
	}
	if c.tableName == "" {
		return nil, errors.NewInvalidStatementError("Drop index requires a table")
	}
	indexInfo, ok := c.e.metaController.GetIndex(c.schemaName, c.tableName, c.indexName)
	if !ok {
		return nil, errors.NewUnknownIndexError(c.schemaName, c.tableName, c.indexName)
	}
	return indexInfo, nil
}
