package command

import (
	"fmt"
	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/meta"
	"github.com/squareup/pranadb/pull"
	"github.com/squareup/pranadb/pull/exec"
	"github.com/squareup/pranadb/push"
	"github.com/squareup/pranadb/storage"
)

type Executor struct {
	store          storage.Storage
	cluster        cluster.Cluster
	metaController *meta.Controller
	pushEngine     *push.PushEngine
	pullEngine     *pull.PullEngine
}

func NewCommandExecutor(store storage.Storage, metaController *meta.Controller, pushEngine *push.PushEngine, pullEngine *pull.PullEngine,
	cluster cluster.Cluster) *Executor {
	return &Executor{
		store:          store,
		cluster:        cluster,
		metaController: metaController,
		pushEngine:     pushEngine,
		pullEngine:     pullEngine,
	}
}

func (p *Executor) CreateSource(schemaName string, name string, colNames []string, colTypes []common.ColumnType, pkCols []int, topicInfo *common.TopicInfo) error {
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

func (p *Executor) ExecutePullQuery(schemaName string, sql string) (exec.PullExecutor, error) {
	schema, ok := p.metaController.GetSchema(schemaName)
	if !ok {
		return nil, fmt.Errorf("unknown schema %s", schemaName)
	}
	dag, err := p.pullEngine.ExecutePullQuery(schema, sql)
	if err != nil {
		return nil, err
	}
	return dag, nil
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
