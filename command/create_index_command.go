package command

import (
	"sync"

	"github.com/squareup/pranadb/command/parser"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/meta"
	"github.com/squareup/pranadb/parplan"
)

type CreateIndexCommand struct {
	lock           sync.Mutex
	e              *Executor
	pl             *parplan.Planner
	schema         *common.Schema
	createIndexSQL string
	tableSequences []uint64
	indexInfo      *common.IndexInfo
	ast            *parser.CreateIndex
}

func (c *CreateIndexCommand) CommandType() DDLCommandType {
	return DDLCommandTypeCreateIndex
}

func (c *CreateIndexCommand) SchemaName() string {
	return c.schema.Name
}

func (c *CreateIndexCommand) SQL() string {
	return c.createIndexSQL
}

func (c *CreateIndexCommand) TableSequences() []uint64 {
	return c.tableSequences
}

func (c *CreateIndexCommand) LockName() string {
	return c.schema.Name + "/"
}

func NewOriginatingCreateIndexCommand(e *Executor, pl *parplan.Planner, schema *common.Schema, createIndexSQL string, ast *parser.CreateIndex) *CreateIndexCommand {
	pl.RefreshInfoSchema()
	return &CreateIndexCommand{
		e:              e,
		schema:         schema,
		pl:             pl,
		createIndexSQL: createIndexSQL,
		ast:            ast,
	}
}

func NewCreateIndexCommand(e *Executor, schemaName string, createIndexSQL string) *CreateIndexCommand {
	schema := e.metaController.GetOrCreateSchema(schemaName)
	pl := parplan.NewPlanner(schema, false)
	return &CreateIndexCommand{
		e:              e,
		schema:         schema,
		pl:             pl,
		createIndexSQL: createIndexSQL,
	}
}

func (c *CreateIndexCommand) BeforePrepare() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	var err error
	c.indexInfo, err = c.getIndexInfo(c.ast)
	if err != nil {
		return errors.WithStack(err)
	}
	// Before prepare we just persist the index info in the indexes table
	return c.e.metaController.PersistIndex(c.indexInfo, meta.PrepareStateAdd)
}

func (c *CreateIndexCommand) OnPrepare() error {
	return nil
}

func (c *CreateIndexCommand) OnCommit() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if err := c.e.pushEngine.CreateIndex(c.indexInfo, true); err != nil {
		return err
	}
	return c.e.metaController.RegisterIndex(c.indexInfo)
}

func (c *CreateIndexCommand) AfterCommit() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.e.metaController.PersistIndex(c.indexInfo, meta.PrepareStateCommitted)
}

func (c *CreateIndexCommand) getIndexInfo(ast *parser.CreateIndex) (*common.IndexInfo, error) {
	var tab common.Table
	tab, ok := c.e.metaController.GetSource(c.SchemaName(), ast.TableName)
	if !ok {
		tab, ok = c.e.metaController.GetMaterializedView(c.SchemaName(), ast.TableName)
		if !ok {
			return nil, errors.NewUnknownSourceOrMaterializedViewError(c.SchemaName(), ast.TableName)
		}
	}
	tabInfo := tab.GetTableInfo()

	if tabInfo.IndexInfos != nil {
		_, ok := tabInfo.IndexInfos[ast.Name]
		if ok {
			return nil, errors.NewIndexAlreadyExistsError(c.SchemaName(), ast.TableName, ast.Name)
		}
	}

	colMap := make(map[string]int, len(tabInfo.ColumnNames))
	for colIndex, colName := range tabInfo.ColumnNames {
		colMap[colName] = colIndex
	}
	indexCols := make([]int, len(ast.ColumnNames))
	for i, colName := range ast.ColumnNames {
		colIndex, ok := colMap[colName.Name]
		if !ok {
			return nil, errors.NewUnknownIndexColumn(c.SchemaName(), ast.TableName, colName.Name)
		}
		indexCols[i] = colIndex
	}
	id, err := c.e.cluster.GenerateClusterSequence("table")
	if err != nil {
		return nil, err
	}
	info := &common.IndexInfo{
		SchemaName: c.SchemaName(),
		ID:         id,
		TableName:  ast.TableName,
		Name:       ast.Name,
		IndexCols:  indexCols,
	}
	return info, nil
}
