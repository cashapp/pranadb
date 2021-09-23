package push

import (
	"fmt"
	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/parplan"
	"github.com/squareup/pranadb/push/exec"
	"github.com/squareup/pranadb/sharder"
	"reflect"
)

type MaterializedView struct {
	pe             *PushEngine
	schema         *common.Schema
	Info           *common.MaterializedViewInfo
	tableExecutor  *exec.TableExecutor
	cluster        cluster.Cluster
	InternalTables []*common.InternalTableInfo
	sharder        *sharder.Sharder
}

// CreateMaterializedView creates the materialized view but does not register it in memory
func CreateMaterializedView(pe *PushEngine, pl *parplan.Planner, schema *common.Schema, mvName string, query string,
	tableID uint64, seqGenerator common.SeqGenerator) (*MaterializedView, error) {
	mv := MaterializedView{
		pe:      pe,
		schema:  schema,
		cluster: pe.cluster,
		sharder: pe.sharder,
	}
	dag, internalTables, err := mv.buildPushQueryExecution(pl, schema, query, mvName, seqGenerator)
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
		ColsVisible:    dag.ColsVisible(),
		IndexInfos:     nil,
	}
	mvInfo := common.MaterializedViewInfo{
		Query:     query,
		TableInfo: &tableInfo,
	}
	mv.Info = &mvInfo
	mv.tableExecutor = exec.NewTableExecutor(&tableInfo, pe.cluster)
	mv.InternalTables = internalTables
	exec.ConnectPushExecutors([]exec.PushExecutor{dag}, mv.tableExecutor)
	return &mv, nil
}

// Connect connects up any executors which consumer data from sources, materialized views, or remote receivers
// to their feeders
func (m *MaterializedView) Connect(addConsuming bool, registerRemote bool) error {
	return m.connect(m.tableExecutor, addConsuming, registerRemote)
}

func (m *MaterializedView) Disconnect() error {
	return m.disconnectOrDeleteDataForMV(m.schema, m.tableExecutor, true, false)
}

func (m *MaterializedView) Drop(deleteData bool) error {
	// Will already have been disconnected
	err := m.disconnectOrDeleteDataForMV(m.schema, m.tableExecutor, false, deleteData)
	if err != nil {
		return err
	}
	if deleteData {
		return m.deleteTableData(m.Info.ID)
	} else {
		return nil
	}
}

func (m *MaterializedView) disconnectOrDeleteDataForMV(schema *common.Schema, node exec.PushExecutor, disconnect bool, deleteData bool) error {

	switch op := node.(type) {
	case *exec.Scan:
		tableName := op.TableName
		tbl, ok := schema.GetTable(tableName)
		if !ok {
			return fmt.Errorf("unknown source or materialized view %s", tableName)
		}
		switch tbl := tbl.(type) {
		case *common.SourceInfo:
			if disconnect {
				source, err := m.pe.GetSource(tbl.ID)
				if err != nil {
					return err
				}
				source.RemoveConsumingExecutor(m.Info.Name)
			}
		case *common.MaterializedViewInfo:
			if disconnect {
				mv, err := m.pe.GetMaterializedView(tbl.ID)
				if err != nil {
					return err
				}
				mv.removeConsumingExecutor(m.Info.Name)
			}
		default:
			return fmt.Errorf("cannot disconnect %s: invalid table type", tbl)
		}
	case *exec.Aggregator:
		if disconnect {
			err := m.pe.UnregisterRemoteConsumer(op.FullAggTableInfo.ID)
			if err != nil {
				return err
			}
		}
		if deleteData {
			err := m.deleteTableData(op.PartialAggTableInfo.ID)
			if err != nil {
				return err
			}
			err = m.deleteTableData(op.FullAggTableInfo.ID)
			if err != nil {
				return err
			}
		}
	}

	for _, child := range node.GetChildren() {
		err := m.disconnectOrDeleteDataForMV(schema, child, disconnect, deleteData)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *MaterializedView) deleteTableData(tableID uint64) error {
	startPrefix := common.AppendUint64ToBufferBE(nil, tableID)
	endPrefix := common.AppendUint64ToBufferBE(nil, tableID+1)
	return m.cluster.DeleteAllDataInRangeForAllShards(startPrefix, endPrefix)
}

func (m *MaterializedView) addConsumingExecutor(mvName string, executor exec.PushExecutor) {
	m.tableExecutor.AddConsumingNode(mvName, executor)
}

func (m *MaterializedView) removeConsumingExecutor(mvName string) {
	m.tableExecutor.RemoveConsumingNode(mvName)
}

func (m *MaterializedView) GetConsumingMVs() []string {
	return m.tableExecutor.GetConsumingMvNames()
}

func (m *MaterializedView) connect(executor exec.PushExecutor, addConsuming bool, registerRemote bool) error {
	for _, child := range executor.GetChildren() {
		err := m.connect(child, addConsuming, registerRemote)
		if err != nil {
			return err
		}
	}
	switch op := executor.(type) {
	case *exec.Scan:
		if addConsuming {
			tableName := op.TableName
			tbl, ok := m.schema.GetTable(tableName)
			if !ok {
				return fmt.Errorf("unknown source or materialized view %s", tableName)
			}
			switch tbl := tbl.(type) {
			case *common.SourceInfo:
				source, err := m.pe.GetSource(tbl.ID)
				if err != nil {
					return err
				}
				source.AddConsumingExecutor(m.Info.Name, executor)
			case *common.MaterializedViewInfo:
				mv, err := m.pe.GetMaterializedView(tbl.ID)
				if err != nil {
					return err
				}
				mv.addConsumingExecutor(m.Info.Name, executor)
			default:
				return fmt.Errorf("table scan on %s is not supported", reflect.TypeOf(tbl))
			}
		}
	case *exec.Aggregator:
		if registerRemote {
			colTypes := op.FullAggTableInfo.ColumnTypes
			rf := common.NewRowsFactory(colTypes)
			rc := &RemoteConsumer{
				RowsFactory: rf,
				ColTypes:    colTypes,
				RowsHandler: op,
			}
			err := m.pe.RegisterRemoteConsumer(op.FullAggTableInfo.ID, rc)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (m *MaterializedView) Fill() error {
	tes, tss, err := m.getFeedingExecutors(m.tableExecutor)
	if err != nil {
		return err
	}

	// TODO if cluster membership changes while fill is in process we need to abort process and start again
	schedulers, err := m.pe.GetLocalLeaderSchedulers()
	if err != nil {
		return err
	}

	chans := make([]chan error, len(tes))

	for i, tableExec := range tes {
		ts := tss[i]
		ch := make(chan error, 1)
		chans[i] = ch
		// Execute in parallel
		te := tableExec
		go func() {
			err := te.FillTo(ts, m.Info.Name, schedulers, m.pe.mover)
			ch <- err
		}()
	}

	for _, ch := range chans {
		err, ok := <-ch
		if !ok {
			panic("channel was closed")
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *MaterializedView) getFeedingExecutors(ex exec.PushExecutor) ([]*exec.TableExecutor, []*exec.Scan, error) {
	var tes []*exec.TableExecutor
	var tss []*exec.Scan
	ts, ok := ex.(*exec.Scan)
	if ok {
		tbl, ok := m.schema.GetTable(ts.TableName)
		if !ok {
			return nil, nil, fmt.Errorf("unknown source or materialized view %s", ts.TableName)
		}
		switch tbl := tbl.(type) {
		case *common.SourceInfo:
			source, err := m.pe.GetSource(tbl.ID)
			if err != nil {
				return nil, nil, err
			}
			tes = append(tes, source.TableExecutor())
		case *common.MaterializedViewInfo:
			mv, err := m.pe.GetMaterializedView(tbl.ID)
			if err != nil {
				return nil, nil, err
			}
			tes = append(tes, mv.tableExecutor)
		}
		tss = append(tss, ts)
	}
	for _, child := range ex.GetChildren() {
		te, ts, err := m.getFeedingExecutors(child)
		if err != nil {
			return nil, nil, err
		}
		tes = append(tes, te...)
		tss = append(tss, ts...)
	}
	return tes, tss, nil
}
