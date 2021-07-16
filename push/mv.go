package push

import (
	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/parplan"
	"github.com/squareup/pranadb/push/exec"
)

type materializedView struct {
	Info          *common.MaterializedViewInfo
	tableExecutor *exec.TableExecutor
	store         cluster.Cluster
}

func (p *PushEngine) CreateMaterializedView(pl *parplan.Planner, schema *common.Schema, mvName string, query string, tableID uint64, seqGenerator common.SeqGenerator) (*common.MaterializedViewInfo, error) {
	dag, err := p.buildPushQueryExecution(pl, schema, query, schema.Name+"."+mvName, seqGenerator)
	if err != nil {
		return nil, err
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

	tableNode := exec.NewTableExecutor(dag.ColTypes(), &tableInfo, p.cluster)

	mv := materializedView{
		Info:          &mvInfo,
		tableExecutor: tableNode,
		store:         p.cluster,
	}
	exec.ConnectPushExecutors([]exec.PushExecutor{dag}, tableNode)
	p.materializedViews[mvInfo.TableInfo.ID] = &mv
	return &mvInfo, nil
}

func (m *materializedView) addConsumingExecutor(node exec.PushExecutor) {
	m.tableExecutor.AddConsumingNode(node)
}
