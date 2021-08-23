package schema

import (
	"fmt"

	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/meta"
	"github.com/squareup/pranadb/parplan"
	"github.com/squareup/pranadb/push"
)

// Loader is a service that loads existing table schemas from disk and applies them to the metadata
// controller and the push engine.
type Loader struct {
	meta       *meta.Controller
	pushEngine *push.PushEngine
	queryExec  common.SimpleQueryExec
}

func NewLoader(m *meta.Controller, push *push.PushEngine, queryExec common.SimpleQueryExec) *Loader {
	return &Loader{
		meta:       m,
		pushEngine: push,
		queryExec:  queryExec,
	}
}

type MVTables struct {
	mvInfo         *common.MaterializedViewInfo
	internalTables []*common.InternalTableInfo
	sequences      []uint64
}

func (l *Loader) Start() error {
	rows, err := l.queryExec.ExecuteQuery("sys",
		"select id, kind, schema_name, name, table_info, topic_info, query, mv_name, prepare_state from tables order by id")
	if err != nil {
		return err
	}

	type tableKey struct {
		schemaName, tableName string
	}
	mvTables := make(map[tableKey]*MVTables)

	for i := 0; i < rows.RowCount(); i++ {
		row := rows.GetRow(i)
		kind := row.GetString(1)
		switch kind {
		case meta.TableKindSource:
			info := meta.DecodeSourceInfoRow(&row)
			// TODO check prepare state and restart command if pending
			if err := l.meta.RegisterSource(info); err != nil {
				return err
			}
			src, err := l.pushEngine.CreateSource(info)
			if err != nil {
				return err
			}
			if err := src.Start(); err != nil {
				return err
			}
		case meta.TableKindMaterializedView:
			info := meta.DecodeMaterializedViewInfoRow(&row)
			tk := tableKey{info.SchemaName, info.Name}
			_, ok := mvTables[tk]
			if ok {
				return fmt.Errorf("mv %s %s already loaded", info.SchemaName, info.Name)
			}
			mvt := &MVTables{mvInfo: info, sequences: []uint64{info.ID}}
			mvTables[tk] = mvt
		case meta.TableKindInternal:
			info := meta.DecodeInternalTableInfoRow(&row)
			tk := tableKey{info.SchemaName, info.MaterializedViewName}
			mvt, ok := mvTables[tk]
			if !ok {
				return fmt.Errorf("mv %s %s not loaded", info.SchemaName, info.Name)
			}
			mvt.sequences = append(mvt.sequences, info.ID)
			mvt.internalTables = append(mvt.internalTables, info)
		default:
			return fmt.Errorf("unknown table kind %s", kind)
		}
	}

	for _, mvt := range mvTables {
		schema := l.meta.GetOrCreateSchema(mvt.mvInfo.SchemaName)
		mv, err := push.CreateMaterializedView(
			l.pushEngine,
			parplan.NewPlanner(schema, false),
			schema, mvt.mvInfo.Name, mvt.mvInfo.Query, mvt.mvInfo.ID,
			common.NewPreallocSeqGen(mvt.sequences))
		if err != nil {
			return err
		}
		if err := mv.Connect(true, true); err != nil {
			return err
		}
		if err := l.pushEngine.RegisterMV(mv); err != nil {
			return err
		}
		if err := l.meta.RegisterMaterializedView(mvt.mvInfo, mvt.internalTables); err != nil {
			return err
		}
	}

	return nil
}

func (l *Loader) Stop() error {
	return nil
}
