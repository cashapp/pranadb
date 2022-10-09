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

type DropSinkCommand struct {
	lock          sync.Mutex
	e             *Executor
	schemaName    string
	sql           string
	sinkName      string
	sink          *push.Sink
	schema        *common.Schema
	toDeleteBatch *cluster.ToDeleteBatch
}

func (d *DropSinkCommand) CommandType() DDLCommandType {
	return DDLCommandTypeDropSink
}

func (d *DropSinkCommand) SchemaName() string {
	return d.schemaName
}

func (d *DropSinkCommand) SQL() string {
	return d.sql
}

func (d *DropSinkCommand) TableSequences() []uint64 {
	return nil
}

func (d *DropSinkCommand) Cancel() {
}

func NewOriginatingDropSinkCommand(e *Executor, schemaName string, sql string, sinkName string) *DropSinkCommand {
	return &DropSinkCommand{
		e:          e,
		schemaName: schemaName,
		sql:        sql,
		sinkName:   strings.ToLower(sinkName),
	}
}

func NewDropSinkCommand(e *Executor, schemaName string, sql string) *DropSinkCommand {
	return &DropSinkCommand{
		e:          e,
		schemaName: schemaName,
		sql:        sql,
	}
}

func (d *DropSinkCommand) Before() error {
	d.lock.Lock()
	defer d.lock.Unlock()

	sink, err := d.getSink()
	if err != nil {
		return errors.WithStack(err)
	}
	d.sink = sink

	return nil
}

func (d *DropSinkCommand) OnPhase(phase int32) error {
	switch phase {
	case 0:
		return d.onPhase0()
	case 1:
		return d.onPhase1()
	default:
		panic("invalid phase")
	}
}

func (d *DropSinkCommand) NumPhases() int {
	return 2
}

func (d *DropSinkCommand) onPhase0() error {
	d.lock.Lock()
	defer d.lock.Unlock()

	if d.sink == nil {
		sink, err := d.getSink()
		if err != nil {
			return errors.WithStack(err)
		}
		d.sink = sink
	}
	if err := d.sink.Stop(); err != nil {
		return err
	}
	if err := d.e.metaController.UnregisterSink(d.schemaName, d.sink.Info.Name); err != nil {
		return errors.WithStack(err)
	}
	schema, ok := d.e.metaController.GetSchema(d.schemaName)
	if !ok {
		return errors.Errorf("no such schema %s", d.schemaName)
	}
	d.schema = schema
	return d.sink.Disconnect()
}

func (d *DropSinkCommand) onPhase1() error {
	d.lock.Lock()
	defer d.lock.Unlock()
	// Remove the sink from the push engine and delete all it's data
	if err := d.e.pushEngine.RemoveSink(d.sink.Info.ID); err != nil {
		return errors.WithStack(err)
	}
	return d.sink.Drop()
}

func (d *DropSinkCommand) AfterPhase(phase int32) error {
	d.lock.Lock()
	defer d.lock.Unlock()
	switch phase {
	case 0:
		var err error
		d.toDeleteBatch, err = storeToDeleteBatch(d.sink.Info.ID, d.e.cluster)
		if err != nil {
			return err
		}

		// Delete the sink from the tables table - we must do this before we delete the data for the sink otherwise we can
		// end up with a partial MV on restart after failure
		return d.e.metaController.DeleteSink(d.sink.Info.ID)
	case 1:
		// Now delete rows from the to_delete table
		return d.e.cluster.RemoveToDeleteBatch(d.toDeleteBatch)
	}
	return nil
}

func (d *DropSinkCommand) Cleanup() {
}

func (d *DropSinkCommand) getSink() (*push.Sink, error) {
	if d.sinkName == "" {
		ast, err := parser.Parse(d.sql)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		if ast.Drop == nil && !ast.Drop.Sink {
			return nil, errors.Errorf("not a drop sink command %s", d.sql)
		}
		d.sinkName = strings.ToLower(ast.Drop.Name)
	}

	sinkInfo, ok := d.e.metaController.GetSink(d.schemaName, d.sinkName)
	if !ok {
		return nil, errors.NewUnknownSinkError(d.schemaName, d.sinkName)
	}
	sink, err := d.e.pushEngine.GetSink(sinkInfo.ID)
	return sink, errors.WithStack(err)
}

func (d *DropSinkCommand) GetExtraData() []byte {
	return nil
}
