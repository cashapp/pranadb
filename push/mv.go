package push

import (
	"github.com/pingcap/tidb/infoschema"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/push/exec"
	"github.com/squareup/pranadb/storage"
)

type materializedView struct {
	Info          *common.MaterializedViewInfo
	tableExecutor *exec.TableExecutor
	store         storage.Storage
}

func (p *PushEngine) CreateMaterializedView(schema *common.Schema, mvName string, query string, tableID uint64, is infoschema.InfoSchema) (*common.MaterializedViewInfo, error) {
	dag, err := p.buildPushQueryExecution(schema, is, query, schema.Name+"."+mvName)
	if err != nil {
		return nil, nil
	}
	tableInfo := common.TableInfo{
		ID:             tableID,
		TableName:      mvName,
		PrimaryKeyCols: dag.KeyCols(),
		ColumnNames:    dag.ColNames(),
		ColumnTypes:    dag.ColTypes(),
		IndexInfos:     nil,
	}
	mvInfo := common.MaterializedViewInfo{
		SchemaName: schema.Name,
		Name:       mvName,
		Query:      query,
		TableInfo:  &tableInfo,
	}
	tableNode, err := exec.NewTableExecutor(dag.ColTypes(), &tableInfo, p.storage)
	if err != nil {
		return nil, nil
	}
	mv := materializedView{
		Info:          &mvInfo,
		tableExecutor: tableNode,
		store:         p.storage,
	}
	exec.ConnectPushExecutors([]exec.PushExecutor{dag}, tableNode)
	p.materializedViews[mvInfo.TableInfo.ID] = &mv
	return &mvInfo, nil
}

func (m *materializedView) addConsumingExecutor(node exec.PushExecutor) {
	m.tableExecutor.AddConsumingNode(node)
}
