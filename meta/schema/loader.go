package schema

import (
	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/interruptor"
	"github.com/squareup/pranadb/meta"
	"github.com/squareup/pranadb/parplan"
	"github.com/squareup/pranadb/push"
)

// Loader is a service that loads existing table schemas from disk and applies them to the metadata
// controller and the push engine. It does not start the sources.
type Loader struct {
	meta       *meta.Controller
	pushEngine *push.Engine
	queryExec  common.SimpleQueryExec
}

func NewLoader(m *meta.Controller, push *push.Engine, queryExec common.SimpleQueryExec) *Loader {
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

func (l *Loader) Start() error { //nolint:gocyclo
	tableRows, err := l.queryExec.ExecuteQuery("sys",
		"select id, kind, schema_name, name, table_info, topic_info, query, mv_name from tables order by id")
	if err != nil {
		return errors.WithStack(err)
	}

	type tableKey struct {
		schemaName, tableName string
	}
	mvTables := make(map[tableKey]*MVTables)

	// MVs must be started in the load order so we maintain a slice
	var mvsToLoad []tableKey

	for i := 0; i < tableRows.RowCount(); i++ {
		tableRow := tableRows.GetRow(i)
		kind := tableRow.GetString(1)
		switch kind {
		case meta.TableKindSource:
			info := meta.DecodeSourceInfoRow(&tableRow)
			if err := l.meta.RegisterSource(info); err != nil {
				return errors.WithStack(err)
			}
			_, err := l.pushEngine.CreateSource(info)
			if err != nil {
				return errors.WithStack(err)
			}
		case meta.TableKindMaterializedView:
			info := meta.DecodeMaterializedViewInfoRow(&tableRow)
			tk := tableKey{info.SchemaName, info.Name}
			_, ok := mvTables[tk]
			if ok {
				return errors.Errorf("mv %s %s already loaded", info.SchemaName, info.Name)
			}
			mvt := &MVTables{mvInfo: info, sequences: []uint64{info.ID}}
			mvTables[tk] = mvt
			mvsToLoad = append(mvsToLoad, tk)
		case meta.TableKindSink:
			info := meta.DecodeSinkInfoRow(&tableRow)
			tk := tableKey{info.SchemaName, info.Name}
			_, ok := mvTables[tk]
			if ok {
				return errors.Errorf("sink %s %s already loaded", info.SchemaName, info.Name)
			}
			if err := l.meta.RegisterSink(info); err != nil {
				return errors.WithStack(err)
			}
			schema := l.meta.GetOrCreateSchema(info.SchemaName)
			pl := parplan.NewPlanner(schema)
			seqGen := common.NewPreallocSeqGen([]uint64{info.ID})
			sink, err := push.CreateSink(l.pushEngine, pl, schema, info.Name, info.Query, info.ID, seqGen, info.TargetInfo)
			if err != nil {
				return errors.WithStack(err)
			}
			if err := l.pushEngine.RegisterSink(sink); err != nil {
				return errors.WithStack(err)
			}
			if err := sink.Start(); err != nil {
				return errors.WithStack(err)
			}
			if err := sink.Connect(); err != nil {
				return errors.WithStack(err)
			}
		case meta.TableKindInternal:
			info := meta.DecodeInternalTableInfoRow(&tableRow)
			tk := tableKey{info.SchemaName, info.MaterializedViewName}
			mvt, ok := mvTables[tk]
			if !ok {
				log.Warnf("found internal table %s but mv %s %s not loaded", info.Name, info.SchemaName, info.MaterializedViewName)
				continue
			}
			mvt.sequences = append(mvt.sequences, info.ID)
			mvt.internalTables = append(mvt.internalTables, info)
		default:
			return errors.Errorf("unknown table kind %s", kind)
		}
	}

	indexRows, err := l.queryExec.ExecuteQuery("sys",
		"select id, schema_name, name, index_info, table_name from indexes order by id")
	if err != nil {
		return errors.WithStack(err)
	}

	for _, tk := range mvsToLoad {
		mvt := mvTables[tk]
		schema := l.meta.GetOrCreateSchema(mvt.mvInfo.SchemaName)
		seqGen := common.NewPreallocSeqGen(mvt.sequences)
		// It's important that we remove the first id from the seq generator as that's the MV id
		// If we don't do this then the internal tables will end up with different ids than last time!
		mvID := seqGen.GenerateSequence()
		mv, err := push.CreateMaterializedView(
			l.pushEngine,
			parplan.NewPlanner(schema),
			schema, mvt.mvInfo.Name, mvt.mvInfo.Query, "", mvID,
			seqGen)
		if err != nil {
			return errors.WithStack(err)
		}
		if err := mv.Connect(true, true); err != nil {
			return errors.WithStack(err)
		}
		if err := l.pushEngine.RegisterMV(mv); err != nil {
			return errors.WithStack(err)
		}
		if err := l.meta.RegisterMaterializedView(mvt.mvInfo, mvt.internalTables); err != nil {
			return errors.WithStack(err)
		}
	}

	for i := 0; i < indexRows.RowCount(); i++ {
		indexRow := indexRows.GetRow(i)
		info := meta.DecodeIndexInfoRow(&indexRow)
		if err := l.pushEngine.CreateIndex(info, false, nil, &interruptor.Interruptor{}); err != nil {
			return err
		}
		if err := l.meta.RegisterIndex(info); err != nil {
			return err
		}
	}

	return nil
}

func (l *Loader) Stop() error {
	return nil
}
