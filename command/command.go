package command

import (
	"github.com/alecthomas/repr"
	"github.com/pkg/errors"

	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/command/parser"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/meta"
	"github.com/squareup/pranadb/pull"
	"github.com/squareup/pranadb/pull/exec"
	"github.com/squareup/pranadb/push"
)

type Executor struct {
	cluster        cluster.Cluster
	metaController *meta.Controller
	pushEngine     *push.PushEngine
	pullEngine     *pull.PullEngine
}

func NewCommandExecutor(
	metaController *meta.Controller,
	pushEngine *push.PushEngine,
	pullEngine *pull.PullEngine,
	cluster cluster.Cluster,
) *Executor {
	return &Executor{
		cluster:        cluster,
		metaController: metaController,
		pushEngine:     pushEngine,
		pullEngine:     pullEngine,
	}
}

func (p *Executor) CreateSource(
	schemaName string,
	name string,
	colNames []string,
	colTypes []common.ColumnType,
	pkCols []int,
	topicInfo *common.TopicInfo,
) error {
	id, err := p.cluster.GenerateTableID()
	if err != nil {
		return err
	}
	tableInfo := common.TableInfo{
		ID:             id,
		TableName:      name,
		PrimaryKeyCols: pkCols,
		ColumnNames:    colNames,
		ColumnTypes:    colTypes,
		IndexInfos:     nil,
	}
	sourceInfo := common.SourceInfo{
		SchemaName: schemaName,
		Name:       name,
		TableInfo:  &tableInfo,
		TopicInfo:  topicInfo,
	}
	err = p.metaController.RegisterSource(&sourceInfo)
	if err != nil {
		return err
	}
	return p.pushEngine.CreateSource(&sourceInfo)
}

func (p *Executor) CreateMaterializedView(schemaName string, name string, query string) error {
	id, err := p.cluster.GenerateTableID()
	if err != nil {
		return err
	}
	schema := p.metaController.GetOrCreateSchema(schemaName)
	mvInfo, err := p.pushEngine.CreateMaterializedView(schema, name, query, id)
	if err != nil {
		return err
	}
	err = p.metaController.RegisterMaterializedView(mvInfo)
	if err != nil {
		return err
	}
	return nil
}

func (p *Executor) CreateSink(schemaName string, sinkInfo *common.SinkInfo) error {
	panic("implement me")
}

func (p *Executor) DropSource(schemaName string, name string) error {
	panic("implement me")
}

func (p *Executor) DropMaterializedView(schemaName string, name string) error {
	panic("implement me")
}

func (p *Executor) DropSink(schemaName string, name string) error {
	panic("implement me")
}

func (p *Executor) CreatePushQuery(sql string) error {
	panic("implement me")
}

// GetPushEngine is only used in testing
func (p *Executor) GetPushEngine() *push.PushEngine {
	return p.pushEngine
}

// ExecuteSQLStatement executes a synchronous SQL statement.
func (p *Executor) ExecuteSQLStatement(schemaName string, sql string) (exec.PullExecutor, error) {
	ast, err := parser.Parse(sql)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	switch {
	case ast.Select != "":
		return p.execSelect(schemaName, ast.Select)

	case ast.Create != nil && ast.Create.MaterializedView != nil:
		return p.execCreateMaterializedView(schemaName, ast.Create.MaterializedView)

	case ast.Create != nil && ast.Create.Source != nil:
		return p.execCreateSource(schemaName, ast.Create.Source)

	default:
		panic("unsupported query " + sql)
	}
}

func (p *Executor) execSelect(schemaName string, sql string) (exec.PullExecutor, error) {
	schema, ok := p.metaController.GetSchema(schemaName)
	if !ok {
		return nil, errors.Errorf("unknown schema %s", schemaName)
	}
	dag, err := p.pullEngine.ExecutePullQuery(schema, sql)
	return dag, errors.WithStack(err)
}

func (p *Executor) execCreateMaterializedView(schemaName string, mv *parser.CreateMaterializedView) (exec.PullExecutor, error) {
	err := p.CreateMaterializedView(schemaName, mv.Name.String(), mv.Query.String())
	return exec.Empty, errors.WithStack(err)
}

func (p *Executor) execCreateSource(schemaName string, src *parser.CreateSource) (exec.PullExecutor, error) {
	var (
		colNames []string
		colTypes []common.ColumnType
		colIndex = map[string]int{}
		pkCols   []int
	)
	for i, option := range src.Options {
		switch {
		case option.Column != nil:
			// Convert AST column definition to a ColumnType.
			col := option.Column
			colIndex[col.Name] = i
			colNames = append(colNames, col.Name)
			colType, err := col.ToColumnType()
			if err != nil {
				return nil, errors.WithStack(err)
			}
			colTypes = append(colTypes, colType)

		case option.PrimaryKey != "":
			index, ok := colIndex[option.PrimaryKey]
			if !ok {
				return nil, errors.Errorf("invalid primary key column %q", option.PrimaryKey)
			}
			pkCols = append(pkCols, index)

		default:
			panic(repr.String(option))
		}
	}
	if err := p.CreateSource(schemaName, src.Name, colNames, colTypes, pkCols, nil); err != nil {
		return nil, errors.WithStack(err)
	}
	return exec.Empty, nil
}
