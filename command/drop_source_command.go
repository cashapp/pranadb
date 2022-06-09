package command

import (
	"github.com/squareup/pranadb/cluster"
	"sync"

	"github.com/squareup/pranadb/command/parser"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/errors"
)

type DropSourceCommand struct {
	lock          sync.Mutex
	e             *Executor
	schemaName    string
	sql           string
	sourceName    string
	sourceInfo    *common.SourceInfo
	toDeleteBatch *cluster.ToDeleteBatch
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

func NewOriginatingDropSourceCommand(e *Executor, schemaName string, sql string, sourceName string) *DropSourceCommand {
	return &DropSourceCommand{
		e:          e,
		schemaName: schemaName,
		sql:        sql,
		sourceName: sourceName,
	}
}

func NewDropSourceCommand(e *Executor, schemaName string, sql string) *DropSourceCommand {
	return &DropSourceCommand{
		e:          e,
		schemaName: schemaName,
		sql:        sql,
	}
}

func (c *DropSourceCommand) Before() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	sourceInfo, err := c.getSourceInfo()
	if err != nil {
		return errors.WithStack(err)
	}
	c.sourceInfo = sourceInfo

	source, err := c.e.pushEngine.GetSource(sourceInfo.ID)
	if err != nil {
		return errors.WithStack(err)
	}
	consuming := source.GetConsumingMVs()
	if len(consuming) != 0 {
		return errors.NewSourceHasChildrenError(c.sourceInfo.SchemaName, c.sourceInfo.Name, consuming)
	}
	return nil
}

func (c *DropSourceCommand) OnPhase(phase int32) error {
	switch phase {
	case 0:
		return c.onPhase0()
	case 1:
		return c.onPhase1()
	default:
		panic("invalid phase")
	}
}

func (c *DropSourceCommand) NumPhases() int {
	return 2
}

func (c *DropSourceCommand) onPhase0() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	// Source should be removed from in memory metadata, consumers for the source should be closed
	if c.sourceInfo == nil {
		sourceInfo, err := c.getSourceInfo()
		if err != nil {
			return errors.WithStack(err)
		}
		c.sourceInfo = sourceInfo
	}
	if err := c.e.metaController.UnregisterSource(c.schemaName, c.sourceInfo.Name); err != nil {
		return errors.WithStack(err)
	}
	src, err := c.e.pushEngine.GetSource(c.sourceInfo.ID)
	if err != nil {
		return errors.WithStack(err)
	}
	// src.Stop() stops the sources consumers, it does not remove it
	return src.Stop()
}

func (c *DropSourceCommand) onPhase1() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	// Remove the source from the push engine and delete all it's data
	// Deleting the data could take some time
	src, err := c.e.pushEngine.RemoveSource(c.sourceInfo)
	if err != nil {
		return errors.WithStack(err)
	}
	return src.Drop()
}

func (c *DropSourceCommand) AfterPhase(phase int32) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	switch phase {
	case 0:
		// We record prefixes in the to_delete table - this makes sure MV data is deleted on restart if failure occurs
		// after this
		var err error
		c.toDeleteBatch, err = storeToDeleteBatch(c.sourceInfo.ID, c.e.cluster)
		if err != nil {
			return err
		}

		// Delete the source from the tables table - this must happen before the source data is deleted or we can
		// end up with partial source on recovery after failure
		return c.e.metaController.DeleteSource(c.sourceInfo.ID)
	case 1:
		// Now delete rows from the to_delete table
		return c.e.cluster.RemoveToDeleteBatch(c.toDeleteBatch)
	}

	return nil
}

func (c *DropSourceCommand) getSourceInfo() (*common.SourceInfo, error) {
	if c.sourceName == "" {
		ast, err := parser.Parse(c.sql)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		if ast.Drop == nil && !ast.Drop.Source {
			return nil, errors.Errorf("not a drop source command %s", c.sql)
		}
		c.sourceName = ast.Drop.Name
	}
	sourceInfo, ok := c.e.metaController.GetSource(c.schemaName, c.sourceName)
	if !ok {
		return nil, errors.NewUnknownSourceError(c.schemaName, c.sourceName)
	}
	return sourceInfo, nil
}
