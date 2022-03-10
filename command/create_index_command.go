package command

import (
	"sync"

	"github.com/squareup/pranadb/command/parser"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/errors"
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

func NewOriginatingCreateIndexCommand(e *Executor, pl *parplan.Planner, schema *common.Schema, createIndexSQL string, tableSequences []uint64, ast *parser.CreateIndex) *CreateIndexCommand {
	pl.RefreshInfoSchema()
	return &CreateIndexCommand{
		e:              e,
		schema:         schema,
		pl:             pl,
		createIndexSQL: createIndexSQL,
		tableSequences: tableSequences,
		ast:            ast,
	}
}

func NewCreateIndexCommand(e *Executor, schemaName string, createIndexSQL string, tableSequences []uint64) *CreateIndexCommand {
	schema := e.metaController.GetOrCreateSchema(schemaName)
	pl := parplan.NewPlanner(schema, false)
	return &CreateIndexCommand{
		e:              e,
		schema:         schema,
		pl:             pl,
		createIndexSQL: createIndexSQL,
		tableSequences: tableSequences,
	}
}

func (c *CreateIndexCommand) BeforeLocal() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	var err error
	c.indexInfo, err = c.getIndexInfo(c.ast)
	return err
}

func (c *CreateIndexCommand) OnPhase(phase int32) error {
	switch phase {
	case 0:
		return c.onPrepare()
	case 1:
		return c.onFill()
	case 2:
		return c.onCommit()
	default:
		panic("invalid phase")
	}
}

func (c *CreateIndexCommand) NumPhases() int {
	return 3
}

func (c *CreateIndexCommand) onPrepare() error {
	return nil
}

func (c *CreateIndexCommand) onFill() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.indexInfo == nil {
		ast, err := parser.Parse(c.createIndexSQL)
		if err != nil {
			return err
		}
		if ast.Create != nil && ast.Create.Index != nil {
			c.indexInfo, err = c.getIndexInfo(ast.Create.Index)
			if err != nil {
				return err
			}
		} else {
			return errors.Errorf("invalid create index statement %s", c.createIndexSQL)
		}
	}
	// We create the index but we don't register it yet
	return c.e.pushEngine.CreateIndex(c.indexInfo, true)
}

func (c *CreateIndexCommand) onCommit() error {
	// Now we register the index so it can be used in queries
	return c.e.metaController.RegisterIndex(c.indexInfo)
}

func (c *CreateIndexCommand) AfterLocal() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.e.metaController.PersistIndex(c.indexInfo)
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
	info := &common.IndexInfo{
		SchemaName: c.SchemaName(),
		ID:         c.tableSequences[0],
		TableName:  ast.TableName,
		Name:       ast.Name,
		IndexCols:  indexCols,
	}
	return info, nil
}
