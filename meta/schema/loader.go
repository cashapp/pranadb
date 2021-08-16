package schema

import (
	"fmt"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/meta"
	"github.com/squareup/pranadb/parplan"
	"github.com/squareup/pranadb/push"
	"log"
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

func (l *Loader) Start() error {
	rows, err := l.queryExec.ExecuteQuery("sys",
		"select id, kind, schema_name, name, table_info, topic_info, query, mv_name, prepare_state from tables order by id")
	if err != nil {
		return err
	}

	var mvs []*common.MaterializedViewInfo
	type tableKey struct {
		schemaName, tableName string
	}
	mvSequences := make(map[tableKey][]uint64)
	var internalTables []*common.InternalTableInfo

	for i := 0; i < rows.RowCount(); i++ {
		row := rows.GetRow(i)
		kind := row.GetString(1)
		switch kind {
		case meta.TableKindSource:
			log.Println("Reading source from storage")
			info := meta.DecodeSourceInfoRow(&row)
			// TODO prepare state!
			if err := l.meta.RegisterSource(info); err != nil {
				return err
			}
			if err := l.pushEngine.CreateSource(info); err != nil {
				return err
			}
			if err := l.pushEngine.StartSource(info.ID); err != nil {
				return err
			}
		case meta.TableKindMaterializedView:
			info := meta.DecodeMaterializedViewInfoRow(&row)
			mvs = append(mvs, info)
			mvSequences[tableKey{info.SchemaName, info.Name}] = []uint64{info.ID}
			log.Printf("Reading mv from storage, id is %d", info.ID)
		case meta.TableKindInternal:

			info := meta.DecodeInternalTableInfoRow(&row)
			key := tableKey{info.SchemaName, info.MaterializedViewName}
			mvSequences[key] = append(mvSequences[key], info.ID)
			log.Printf("Reading internal table from storage, id is %d", info.ID)
			internalTables = append(internalTables, info)
		default:
			return fmt.Errorf("unknown table kind %s", kind)
		}
	}

	for _, internalInfo := range internalTables {
		if err := l.meta.RegisterInternalTable(internalInfo, false); err != nil {
			return err
		}
	}
	for _, mvInfo := range mvs {
		schema := l.meta.GetOrCreateSchema(mvInfo.SchemaName)
		mv, err := l.pushEngine.CreateMaterializedView(
			parplan.NewPlanner(schema, false),
			schema, mvInfo.Name, mvInfo.Query, mvInfo.ID,
			common.NewPreallocSeqGen(mvSequences[tableKey{mvInfo.SchemaName, mvInfo.Name}]))
		if err := l.pushEngine.ConnectMV(mv); err != nil {
			return err
		}
		if err := l.pushEngine.RegisterMV(mv); err != nil {
			return err
		}
		if err := l.meta.RegisterMaterializedView(mvInfo, false); err != nil {
			return err
		}

		if err != nil {
			return err
		}
	}

	return nil
}

func (l *Loader) Stop() error {
	return nil
}
