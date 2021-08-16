package push

import (
	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/parplan"
	"github.com/squareup/pranadb/push/exec"
)

type MaterializedView struct {
	p              *PushEngine
	schema         *common.Schema
	Info           *common.MaterializedViewInfo
	tableExecutor  *exec.TableExecutor
	store          cluster.Cluster
	InternalTables []*common.InternalTableInfo
}

// CreateMaterializedView creates the materialized view but does not register it in memory
func (p *PushEngine) CreateMaterializedView(pl *parplan.Planner, schema *common.Schema, mvName string, query string,
	tableID uint64, seqGenerator common.SeqGenerator) (*MaterializedView, error) {
	dag, internalTables, err := p.buildPushQueryExecution(pl, schema, query, mvName, seqGenerator)
	if err != nil {
		return nil, err
	}
	tableInfo := common.TableInfo{
		ID:             tableID,
		SchemaName:     schema.Name,
		Name:           mvName,
		PrimaryKeyCols: dag.KeyCols(),
		ColumnNames:    dag.ColNames(),
		ColumnTypes:    dag.ColTypes(),
		IndexInfos:     nil,
	}
	mvInfo := common.MaterializedViewInfo{
		Query:     query,
		TableInfo: &tableInfo,
	}

	tableNode := exec.NewTableExecutor(dag.ColTypes(), &tableInfo, p.cluster)

	mv := MaterializedView{
		p:              p,
		schema:         schema,
		Info:           &mvInfo,
		tableExecutor:  tableNode,
		store:          p.cluster,
		InternalTables: internalTables,
	}
	exec.ConnectPushExecutors([]exec.PushExecutor{dag}, tableNode)
	return &mv, nil
}

// start connects up the mv to it's sources so it can start receiving rows
func (m *MaterializedView) start() error {
	return m.p.connectToFeeders(m.tableExecutor, m.schema)
}

func (m *MaterializedView) stop() {
	// TODO disconnect from feeders
}

func (m *MaterializedView) register() {

}

//
//func (p *PushEngine) CreateMaterializedView(pl *parplan.Planner, schema *common.Schema, mvName string, query string,
//	tableID uint64, seqGenerator common.SeqGenerator) (*common.MaterializedViewInfo, error) {
//	dag, err := p.buildPushQueryExecution(pl, schema, query, mvName, seqGenerator)
//	if err != nil {
//		return nil, err
//	}
//	tableInfo := common.TableInfo{
//		ID:             tableID,
//		SchemaName:     schema.Name,
//		Name:           mvName,
//		PrimaryKeyCols: dag.KeyCols(),
//		ColumnNames:    dag.ColNames(),
//		ColumnTypes:    dag.ColTypes(),
//		IndexInfos:     nil,
//	}
//	mvInfo := common.MaterializedViewInfo{
//		Query:     query,
//		TableInfo: &tableInfo,
//	}
//
//	tableNode := exec.NewTableExecutor(dag.ColTypes(), &tableInfo, p.cluster)
//
//	mv := materializedView{
//		Info:          &mvInfo,
//		tableExecutor: tableNode,
//		store:         p.cluster,
//	}
//	exec.ConnectPushExecutors([]exec.PushExecutor{dag}, tableNode)
//	p.materializedViews[mvInfo.TableInfo.ID] = &mv
//	return &mvInfo, nil
//}

func (m *MaterializedView) addConsumingExecutor(node exec.PushExecutor) {
	m.tableExecutor.AddConsumingNode(node)
}

func (m *MaterializedView) removeConsumingExecutor(executor exec.PushExecutor) {
	m.tableExecutor.RemoveConsumingNode(executor)
}
