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

func (d *DropIndexCommand) CommandType() DDLCommandType {
	return DDLCommandTypeDropIndex
}

func (d *DropIndexCommand) SchemaName() string {
	return d.schemaName
}

func (d *DropIndexCommand) SQL() string {
	return d.sql
}

func (d *DropIndexCommand) TableSequences() []uint64 {
	return nil
}

func (d *DropIndexCommand) Cancel() {
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

func (d *DropIndexCommand) Before() error {
	d.lock.Lock()
	defer d.lock.Unlock()

	indexInfo, err := d.getIndexInfo()
	if err != nil {
		return errors.WithStack(err)
	}
	d.indexInfo = indexInfo
	return nil
}

func (d *DropIndexCommand) OnPhase(phase int32) error {
	switch phase {
	case 0:
		return d.onPhase0()
	case 1:
		return d.onPhase1()
	default:
		panic("invalid phase")
	}
}

func (d *DropIndexCommand) NumPhases() int {
	return 2
}

func (d *DropIndexCommand) onPhase0() error {
	d.lock.Lock()
	defer d.lock.Unlock()

	if d.indexInfo == nil {
		indexInfo, err := d.getIndexInfo()
		if err != nil {
			return errors.WithStack(err)
		}
		d.indexInfo = indexInfo
	}
	if err := d.e.metaController.UnregisterIndex(d.schemaName, d.indexInfo.TableName, d.indexInfo.Name); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (d *DropIndexCommand) onPhase1() error {
	d.lock.Lock()
	defer d.lock.Unlock()
	// This disconnects and removes the data for the index (data removal only happens on the originating node)
	return d.e.pushEngine.RemoveIndex(d.indexInfo)
}

func (d *DropIndexCommand) AfterPhase(phase int32) error {
	d.lock.Lock()
	defer d.lock.Unlock()

	switch phase {
	case 0:
		// We record prefixes in the to_delete table - this makes sure index data is deleted on restart if failure occurs
		// after this
		var err error
		d.toDeleteBatch, err = storeToDeleteBatch(d.indexInfo.ID, d.e.cluster)
		if err != nil {
			return err
		}

		// Delete the index info from the indexes table - we must do this before we start deleting the actual index data
		// otherwise we can end up with a partial index if failure occurs
		if err := d.e.metaController.DeleteIndex(d.indexInfo.ID); err != nil {
			return err
		}
	case 1:
		// Now delete rows from the to_delete table
		return d.e.cluster.RemoveToDeleteBatch(d.toDeleteBatch)
	}
	return nil
}

func (d *DropIndexCommand) Cleanup() {
}

func (d *DropIndexCommand) getIndexInfo() (*common.IndexInfo, error) {
	if d.indexName == "" {
		ast, err := parser.Parse(d.sql)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		if ast.Drop == nil && !ast.Drop.Index {
			return nil, errors.Errorf("not a drop index command %s", d.sql)
		}
		d.indexName = strings.ToLower(ast.Drop.Name)
		d.tableName = strings.ToLower(ast.Drop.TableName)
	}
	if d.tableName == "" {
		return nil, errors.NewInvalidStatementError("Drop index requires a table")
	}
	indexInfo, ok := d.e.metaController.GetIndex(d.schemaName, d.tableName, d.indexName)
	if !ok {
		return nil, errors.NewUnknownIndexError(d.schemaName, d.tableName, d.indexName)
	}
	return indexInfo, nil
}
