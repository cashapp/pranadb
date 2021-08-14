package command

import (
	"fmt"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/meta"
	"strings"
	"sync"
)

type DropSourceCommand struct {
	lock       sync.Mutex
	e          *Executor
	schemaName string
	sql        string
	sourceInfo *common.SourceInfo
}

func (c *DropSourceCommand) CommandType() DDLCommandType {
	return DDLCommandTypeDropSource
}

func (c *DropSourceCommand) SchemaName() string {
	return c.schemaName
}

func (c *DropSourceCommand) SQL() string {
	return c.sql
}

func (c *DropSourceCommand) TableSequences() []uint64 {
	return nil
}

func (c *DropSourceCommand) LockName() string {
	return c.schemaName + "/"
}

func NewDropSourceCommand(e *Executor, schemaName string, sql string) *DropSourceCommand {
	return &DropSourceCommand{
		e:          e,
		schemaName: schemaName,
		sql:        sql,
	}
}

func (c *DropSourceCommand) BeforePrepare() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	sourceInfo, err := c.getSourceInfo()
	if err != nil {
		return err
	}
	c.sourceInfo = sourceInfo

	// Update row in tables table to mark it as pending delete
	return c.e.metaController.PersistSource(sourceInfo, meta.PrepareStateDelete)
}

func (c *DropSourceCommand) OnPrepare() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	// Source should be removed from in memory metadata, consumers for the source should be closed
	if c.sourceInfo == nil {
		sourceInfo, err := c.getSourceInfo()
		if err != nil {
			return err
		}
		c.sourceInfo = sourceInfo
	}
	if err := c.e.metaController.UnregisterSource(c.schemaName, c.sourceInfo.Name); err != nil {
		return err
	}
	src, err := c.e.pushEngine.GetSource(c.sourceInfo.ID)
	if err != nil {
		return err
	}
	// src.Stop() stops the sources consumers, it does not remove it
	return src.Stop()
}

func (c *DropSourceCommand) OnCommit() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	// Remove the source from the push engine and delete all it's data
	// Deleting the data could take some time
	return c.e.pushEngine.RemoveSource(c.sourceInfo, true)
}

func (c *DropSourceCommand) AfterCommit() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	// Delete the source from the tables table
	return c.e.metaController.DeleteSource(c.sourceInfo.ID)
}

func (c *DropSourceCommand) getSourceInfo() (*common.SourceInfo, error) {
	// TODO we should really use the parser to do this
	parts := strings.Split(c.sql, " ")
	if len(parts) < 3 {
		return nil, errors.MaybeAddStack(fmt.Errorf("invalid drop statement %s", c.sql))
	}
	sourceName := parts[2]
	sourceInfo, ok := c.e.metaController.GetSource(c.schemaName, sourceName)
	if !ok {
		return nil, errors.MaybeAddStack(fmt.Errorf("unknown source %s", sourceName))
	}
	return sourceInfo, nil
}
