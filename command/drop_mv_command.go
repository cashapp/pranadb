package command

import (
	"sync"

	"github.com/squareup/pranadb/command/parser"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/push"
)

type DropMVCommand struct {
	lock             sync.Mutex
	e                *Executor
	schemaName       string
	sql              string
	mvName           string
	mv               *push.MaterializedView
	schema           *common.Schema
	originating      bool
	prefixesToDelete [][]byte
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

func (c *DropMVCommand) Before() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	mv, err := c.getMV()
	if err != nil {
		return errors.WithStack(err)
	}
	c.mv = mv

	consuming := c.mv.GetConsumingMVs()
	if len(consuming) != 0 {
		return errors.NewMaterializedViewHasChildrenError(mv.Info.SchemaName, mv.Info.Name, consuming)
	}
	return nil
}

func (c *DropMVCommand) OnPhase(phase int32) error {
	switch phase {
	case 0:
		return c.onPhase0()
	case 1:
		return c.onPhase1()
	default:
		panic("invalid phase")
	}
}

func (c *DropMVCommand) NumPhases() int {
	return 2
}

func (c *DropMVCommand) onPhase0() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	// MV should be removed from in memory metadata and disconnected from it's sources and
	// any aggregate tables disconnected as remote receivers
	if c.mv == nil {
		mv, err := c.getMV()
		if err != nil {
			return errors.WithStack(err)
		}
		c.mv = mv
	}
	var itNames []string
	for _, it := range c.mv.InternalTables {
		itNames = append(itNames, it.Name)
	}
	if err := c.e.metaController.UnregisterMaterializedView(c.schemaName, c.mv.Info.Name, itNames); err != nil {
		return errors.WithStack(err)
	}
	schema, ok := c.e.metaController.GetSchema(c.schemaName)
	if !ok {
		return errors.Errorf("no such schema %s", c.schemaName)
	}
	c.schema = schema
	return c.mv.Disconnect()
}

func (c *DropMVCommand) onPhase1() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	// Remove the mv from the push engine and delete all it's data
	// Deleting the data could take some time
	if err := c.e.pushEngine.RemoveMV(c.mv.Info.ID); err != nil {
		return errors.WithStack(err)
	}
	// We only delete the data from the originating node - otherwise all nodes would be deleting the same data
	return c.mv.Drop(c.originating)
}

func (c *DropMVCommand) AfterPhase(phase int32) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	switch phase {
	case 0:
		var err error
		c.prefixesToDelete, err = storeToDeletePrefixes(c.mv.Info.ID, c.e.cluster)
		if err != nil {
			return err
		}

		// Delete the mv from the tables table - we must do this before we delete the data for the MV otherwise we can
		// end up with a partial MV on restart after failure
		return c.e.metaController.DeleteMaterializedView(c.mv.Info, c.mv.InternalTables)
	case 1:
		// Now delete rows from the to_delete table
		return c.e.cluster.RemovePrefixesToDelete(false, c.prefixesToDelete...)
	}
	return nil
}

func (c *DropMVCommand) getMV() (*push.MaterializedView, error) {
	if c.mvName == "" {
		ast, err := parser.Parse(c.sql)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		if ast.Drop == nil && !ast.Drop.MaterializedView {
			return nil, errors.Errorf("not a drop materialized view command %s", c.sql)
		}
		c.mvName = ast.Drop.Name
	}

	mvInfo, ok := c.e.metaController.GetMaterializedView(c.schemaName, c.mvName)
	if !ok {
		return nil, errors.NewUnknownMaterializedViewError(c.schemaName, c.mvName)
	}
	mv, err := c.e.pushEngine.GetMaterializedView(mvInfo.ID)
	return mv, errors.WithStack(err)
}
