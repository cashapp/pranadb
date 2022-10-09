package command

import (
	"github.com/squareup/pranadb/cluster"
	"strings"
	"sync"

	"github.com/squareup/pranadb/command/parser"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/push"
)

type DropMVCommand struct {
	lock          sync.Mutex
	e             *Executor
	schemaName    string
	sql           string
	mvName        string
	mv            *push.MaterializedView
	schema        *common.Schema
	toDeleteBatch *cluster.ToDeleteBatch
}

func (d *DropMVCommand) CommandType() DDLCommandType {
	return DDLCommandTypeDropMV
}

func (d *DropMVCommand) SchemaName() string {
	return d.schemaName
}

func (d *DropMVCommand) SQL() string {
	return d.sql
}

func (d *DropMVCommand) TableSequences() []uint64 {
	return nil
}

func (d *DropMVCommand) Cancel() {
}

func NewOriginatingDropMVCommand(e *Executor, schemaName string, sql string, mvName string) *DropMVCommand {
	return &DropMVCommand{
		e:          e,
		schemaName: schemaName,
		sql:        sql,
		mvName:     strings.ToLower(mvName),
	}
}

func NewDropMVCommand(e *Executor, schemaName string, sql string) *DropMVCommand {
	return &DropMVCommand{
		e:          e,
		schemaName: schemaName,
		sql:        sql,
	}
}

func (d *DropMVCommand) Before() error {
	d.lock.Lock()
	defer d.lock.Unlock()

	mv, err := d.getMV()
	if err != nil {
		return errors.WithStack(err)
	}
	d.mv = mv

	consuming := d.mv.GetConsumingMVOrIndexNames()
	if len(consuming) != 0 {
		return errors.NewMaterializedViewHasChildrenError(mv.Info.SchemaName, mv.Info.Name, consuming)
	}
	return nil
}

func (d *DropMVCommand) OnPhase(phase int32) error {
	switch phase {
	case 0:
		return d.onPhase0()
	case 1:
		return d.onPhase1()
	default:
		panic("invalid phase")
	}
}

func (d *DropMVCommand) NumPhases() int {
	return 2
}

func (d *DropMVCommand) onPhase0() error {
	d.lock.Lock()
	defer d.lock.Unlock()

	// MV should be removed from in memory metadata and disconnected from it's sources and
	// any aggregate tables disconnected as remote receivers
	if d.mv == nil {
		mv, err := d.getMV()
		if err != nil {
			return errors.WithStack(err)
		}
		d.mv = mv
	}
	var itNames []string
	for _, it := range d.mv.InternalTables {
		itNames = append(itNames, it.Name)
	}
	if err := d.e.metaController.UnregisterMaterializedView(d.schemaName, d.mv.Info.Name, itNames); err != nil {
		return errors.WithStack(err)
	}
	schema, ok := d.e.metaController.GetSchema(d.schemaName)
	if !ok {
		return errors.Errorf("no such schema %s", d.schemaName)
	}
	d.schema = schema
	return d.mv.Disconnect()
}

func (d *DropMVCommand) onPhase1() error {
	d.lock.Lock()
	defer d.lock.Unlock()
	// Remove the sink from the push engine and delete all it's data
	// Deleting the data could take some time
	if err := d.e.pushEngine.RemoveMV(d.mv.Info.ID); err != nil {
		return errors.WithStack(err)
	}
	return d.mv.Drop()
}

func (d *DropMVCommand) AfterPhase(phase int32) error {
	d.lock.Lock()
	defer d.lock.Unlock()
	switch phase {
	case 0:
		var err error
		d.toDeleteBatch, err = storeToDeleteBatch(d.mv.Info.ID, d.e.cluster)
		if err != nil {
			return err
		}

		// Delete the sink from the tables table - we must do this before we delete the data for the MV otherwise we can
		// end up with a partial MV on restart after failure
		return d.e.metaController.DeleteMaterializedView(d.mv.Info, d.mv.InternalTables)
	case 1:
		// Now delete rows from the to_delete table
		return d.e.cluster.RemoveToDeleteBatch(d.toDeleteBatch)
	}
	return nil
}

func (d *DropMVCommand) Cleanup() {
}

func (d *DropMVCommand) getMV() (*push.MaterializedView, error) {
	if d.mvName == "" {
		ast, err := parser.Parse(d.sql)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		if ast.Drop == nil && !ast.Drop.MaterializedView {
			return nil, errors.Errorf("not a drop materialized view command %s", d.sql)
		}
		d.mvName = strings.ToLower(ast.Drop.Name)
	}

	mvInfo, ok := d.e.metaController.GetMaterializedView(d.schemaName, d.mvName)
	if !ok {
		return nil, errors.NewUnknownMaterializedViewError(d.schemaName, d.mvName)
	}
	mv, err := d.e.pushEngine.GetMaterializedView(mvInfo.ID)
	return mv, errors.WithStack(err)
}

func (d *DropMVCommand) GetExtraData() []byte {
	return nil
}
