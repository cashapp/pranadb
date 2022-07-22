package command

import (
	"github.com/squareup/pranadb/cluster"
	"strings"
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

func (d *DropSourceCommand) CommandType() DDLCommandType {
	return DDLCommandTypeDropSource
}

func (d *DropSourceCommand) SchemaName() string {
	return d.schemaName
}

func (d *DropSourceCommand) SQL() string {
	return d.sql
}

func (d *DropSourceCommand) TableSequences() []uint64 {
	return nil
}

func (d *DropSourceCommand) Cancel() {
}

func NewOriginatingDropSourceCommand(e *Executor, schemaName string, sql string, sourceName string) *DropSourceCommand {
	return &DropSourceCommand{
		e:          e,
		schemaName: schemaName,
		sql:        sql,
		sourceName: strings.ToLower(sourceName),
	}
}

func NewDropSourceCommand(e *Executor, schemaName string, sql string) *DropSourceCommand {
	return &DropSourceCommand{
		e:          e,
		schemaName: schemaName,
		sql:        sql,
	}
}

func (d *DropSourceCommand) Before() error {
	d.lock.Lock()
	defer d.lock.Unlock()

	sourceInfo, err := d.getSourceInfo()
	if err != nil {
		return errors.WithStack(err)
	}
	d.sourceInfo = sourceInfo

	source, err := d.e.pushEngine.GetSource(sourceInfo.ID)
	if err != nil {
		return errors.WithStack(err)
	}
	consuming := source.GetConsumingMVs()
	if len(consuming) != 0 {
		return errors.NewSourceHasChildrenError(d.sourceInfo.SchemaName, d.sourceInfo.Name, consuming)
	}
	return nil
}

func (d *DropSourceCommand) OnPhase(phase int32) error {
	switch phase {
	case 0:
		return d.onPhase0()
	case 1:
		return d.onPhase1()
	default:
		panic("invalid phase")
	}
}

func (d *DropSourceCommand) NumPhases() int {
	return 2
}

func (d *DropSourceCommand) onPhase0() error {
	d.lock.Lock()
	defer d.lock.Unlock()

	// Source should be removed from in memory metadata, consumers for the source should be closed
	if d.sourceInfo == nil {
		sourceInfo, err := d.getSourceInfo()
		if err != nil {
			return errors.WithStack(err)
		}
		d.sourceInfo = sourceInfo
	}
	if err := d.e.metaController.UnregisterSource(d.schemaName, d.sourceInfo.Name); err != nil {
		return errors.WithStack(err)
	}
	src, err := d.e.pushEngine.GetSource(d.sourceInfo.ID)
	if err != nil {
		return errors.WithStack(err)
	}
	// src.Stop() stops the sources consumers, it does not remove it
	return src.Stop()
}

func (d *DropSourceCommand) onPhase1() error {
	d.lock.Lock()
	defer d.lock.Unlock()

	// Remove the source from the push engine and delete all it's data
	// Deleting the data could take some time
	src, err := d.e.pushEngine.RemoveSource(d.sourceInfo)
	if err != nil {
		return errors.WithStack(err)
	}
	return src.Drop()
}

func (d *DropSourceCommand) AfterPhase(phase int32) error {
	d.lock.Lock()
	defer d.lock.Unlock()

	switch phase {
	case 0:
		// We record prefixes in the to_delete table - this makes sure MV data is deleted on restart if failure occurs
		// after this
		var err error
		d.toDeleteBatch, err = storeToDeleteBatch(d.sourceInfo.ID, d.e.cluster)
		if err != nil {
			return err
		}

		// Delete the source from the tables table - this must happen before the source data is deleted or we can
		// end up with partial source on recovery after failure
		return d.e.metaController.DeleteSource(d.sourceInfo.ID)
	case 1:
		// Now delete rows from the to_delete table
		return d.e.cluster.RemoveToDeleteBatch(d.toDeleteBatch)
	}

	return nil
}

func (d *DropSourceCommand) Cleanup() {
}

func (d *DropSourceCommand) getSourceInfo() (*common.SourceInfo, error) {
	if d.sourceName == "" {
		ast, err := parser.Parse(d.sql)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		if ast.Drop == nil && !ast.Drop.Source {
			return nil, errors.Errorf("not a drop source command %s", d.sql)
		}
		d.sourceName = strings.ToLower(ast.Drop.Name)
	}
	sourceInfo, ok := d.e.metaController.GetSource(d.schemaName, d.sourceName)
	if !ok {
		return nil, errors.NewUnknownSourceError(d.schemaName, d.sourceName)
	}
	return sourceInfo, nil
}

func (d *DropSourceCommand) GetExtraData() []byte {
	return nil
}
