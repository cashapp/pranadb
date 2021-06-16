package pranadb

import (
	"github.com/pingcap/tidb/infoschema"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/exec"
	planner2 "github.com/squareup/pranadb/parplan"
	"github.com/squareup/pranadb/storage"
)

type MaterializedView struct {
	SchemaName    string
	Name          string
	Query         string
	Table         common.Table
	TableExecutor *exec.TableExecutor
	store         storage.Storage
}

func NewMaterializedView(mvName string, query string, mvTableID uint64, schema *Schema,
	is infoschema.InfoSchema, storage storage.Storage, planner planner2.Planner) (*MaterializedView, error) {
	dag, err := BuildPushQueryExecution(schema, is, query, planner)
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
	mvTable, err := common.NewTable(storage, &tableInfo)
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
