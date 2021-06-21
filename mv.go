package pranadb

import (
	"github.com/pingcap/tidb/infoschema"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/exec"
	planner2 "github.com/squareup/pranadb/parplan"
	"github.com/squareup/pranadb/storage"
	"github.com/squareup/pranadb/table"
)

type MaterializedView struct {
	SchemaName    string
	Name          string
	Query         string
	Table         table.Table
	TableExecutor *exec.TableExecutor
	store         storage.Storage
}

func NewMaterializedView(mvName string, query string, mvTableID uint64, schema *Schema,
	is infoschema.InfoSchema, storage storage.Storage, planner planner2.Planner,
	remoteConsumers map[uint64]*remoteConsumer, tableIDGenerator TableIDGenerator,
	queryName string, store storage.Storage, sharder common.Sharder) (*MaterializedView, error) {

	dag, err := BuildPushQueryExecution(schema, is, query, planner, remoteConsumers, tableIDGenerator, queryName, store, sharder)

	if err != nil {
		return nil, err
	}
	tableInfo := common.TableInfo{
		ID:             mvTableID,
		TableName:      mvName,
		ColumnNames:    dag.ColNames(),
		ColumnTypes:    dag.ColTypes(),
		PrimaryKeyCols: dag.KeyCols(),
	}
	mvTable, err := table.NewTable(storage, &tableInfo)
	if err != nil {
		return nil, err
	}
	tableNode, err := exec.NewTableExecutor(dag.ColTypes(), mvTable, storage)
	if err != nil {
		return nil, err
	}
	mv := MaterializedView{
		SchemaName:    schema.Name,
		Name:          mvName,
		Query:         query,
		Table:         mvTable,
		TableExecutor: tableNode,
		store:         storage,
	}
	exec.ConnectExecutors([]exec.PushExecutor{dag}, tableNode)
	return &mv, nil
}

func (m *MaterializedView) AddConsumingExecutor(node exec.PushExecutor) {
	m.TableExecutor.AddConsumingNode(node)
}
