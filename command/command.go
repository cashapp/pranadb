package command

import (
	"log"
	"sync"
	"sync/atomic"

	"github.com/alecthomas/repr"
	"github.com/pkg/errors"
	"github.com/squareup/pranadb/parplan"
	"github.com/squareup/pranadb/sess"

	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/command/parser"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/meta"
	"github.com/squareup/pranadb/protos/squareup/cash/pranadb/v1/notifications"
	"github.com/squareup/pranadb/pull"
	"github.com/squareup/pranadb/pull/exec"
	"github.com/squareup/pranadb/push"
)

type Executor struct {
	cluster         cluster.Cluster
	metaController  *meta.Controller
	pushEngine      *push.PushEngine
	pullEngine      *pull.PullEngine
	ddlStatementSeq int64
	notifActions    map[int64]chan statementExecutionResult
	ddlExecPlanner  *parplan.Planner // Only used for executing DDL
	ddlExecLock     sync.Mutex
}

func NewCommandExecutor(
	metaController *meta.Controller,
	pushEngine *push.PushEngine,
	pullEngine *pull.PullEngine,
	cluster cluster.Cluster,
) *Executor {
	return &Executor{
		cluster:        cluster,
		metaController: metaController,
		pushEngine:     pushEngine,
		pullEngine:     pullEngine,
		notifActions:   make(map[int64]chan statementExecutionResult),
		ddlExecPlanner: parplan.NewPlanner(),
	}
}

// ExecuteSQLStatement executes a synchronous SQL statement.
func (e *Executor) ExecuteSQLStatement(session *sess.Session, sql string) (exec.PullExecutor, error) {
	return e.executeSQLStatementInternal(session, sql, true, nil)
}

func (e *Executor) createSource(schemaName string, name string, colNames []string, colTypes []common.ColumnType, pkCols []int, topicInfo *common.TopicInfo, seqGenerator common.SeqGenerator, persist bool) error {
	log.Printf("creating source %s on node %d", name, e.cluster.GetNodeID())
	id := seqGenerator.GenerateSequence()

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
	err := e.metaController.RegisterSource(&sourceInfo, persist)
	if err != nil {
		return err
	}
	return e.pushEngine.CreateSource(&sourceInfo)
}

func (e *Executor) createMaterializedView(session *sess.Session, name string, query string, seqGenerator common.SeqGenerator) error {
	log.Printf("creating mv %s on node %d", name, e.cluster.GetNodeID())
	id := seqGenerator.GenerateSequence()
	mvInfo, err := e.pushEngine.CreateMaterializedView(session, name, query, id, seqGenerator)
	if err != nil {
		return err
	}
	err = e.metaController.RegisterMaterializedView(mvInfo)
	if err != nil {
		return err
	}
	return nil
}

// GetPushEngine is only used in testing
func (e *Executor) GetPushEngine() *push.PushEngine {
	return e.pushEngine
}

func (e *Executor) executeSQLStatementInternal(session *sess.Session, sql string, local bool, seqGenerator common.SeqGenerator) (exec.PullExecutor, error) {
	ast, err := parser.Parse(sql)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	switch {
	case ast.Select != "":
		return e.execSelect(session, ast.Select)

	case ast.Create != nil && ast.Create.MaterializedView != nil:
		if local {
			// Materialized view needs 2 sequences
			sequences, err := e.generateSequences(2)
			if err != nil {
				return nil, err
			}
			return e.executeInGlobalOrder(session.Schema.Name, sql, "mv", sequences)
		}
		return e.execCreateMaterializedView(session, ast.Create.MaterializedView, seqGenerator)

	case ast.Create != nil && ast.Create.Source != nil:
		if local {
			// Source needs 1 sequence
			sequences, err := e.generateSequences(1)
			if err != nil {
				return nil, err
			}
			_, err = e.execCreateSource(session.Schema.Name, ast.Create.Source, &preallocSeqGen{sequences: sequences}, true)
			if err != nil {
				return nil, err
			}
			return e.executeInGlobalOrder(session.Schema.Name, sql, "source", sequences)
		}
		return e.execCreateSource(session.Schema.Name, ast.Create.Source, seqGenerator, false)

	default:
		panic("unsupported query " + sql)
	}
}

func (e *Executor) generateSequences(numValues int) ([]uint64, error) {
	sequences := make([]uint64, numValues)
	for i := 0; i < numValues; i++ {
		v, err := e.cluster.GenerateTableID()
		if err != nil {
			return nil, err
		}
		sequences[i] = v
	}
	return sequences, nil
}

// We need to reserve the table sequences required for the DDL statement *before* we broadcast the DDL across the
// cluster, and those same table sequence values have to be used on every node for consistency.
// So we create a sequence generator that returns value based on already obtained sequence values
type preallocSeqGen struct {
	sequences []uint64
	index     int
}

func (p preallocSeqGen) GenerateSequence() uint64 {
	if p.index >= len(p.sequences) {
		panic("not enough sequence values")
	}
	res := p.sequences[p.index]
	p.index++
	return res
}

func (e *Executor) execSelect(session *sess.Session, sql string) (exec.PullExecutor, error) {
	dag, err := e.pullEngine.BuildPullQuery(session, sql)
	return dag, errors.WithStack(err)
}

func (e *Executor) execCreateMaterializedView(session *sess.Session, mv *parser.CreateMaterializedView, seqGenerator common.SeqGenerator) (exec.PullExecutor, error) {
	err := e.createMaterializedView(session, mv.Name.String(), mv.Query.String(), seqGenerator)
	return exec.Empty, errors.WithStack(err)
}

func (e *Executor) execCreateSource(schemaName string, src *parser.CreateSource, seqGenerator common.SeqGenerator, persist bool) (exec.PullExecutor, error) {
	var (
		colNames []string
		colTypes []common.ColumnType
		colIndex = map[string]int{}
		pkCols   []int
	)
	for i, option := range src.Options {
		switch {
		case option.Column != nil:
			// Convert AST column definition to a ColumnType.
			col := option.Column
			colIndex[col.Name] = i
			colNames = append(colNames, col.Name)
			colType, err := col.ToColumnType()
			if err != nil {
				return nil, errors.WithStack(err)
			}
			colTypes = append(colTypes, colType)

		case option.PrimaryKey != "":
			index, ok := colIndex[option.PrimaryKey]
			if !ok {
				return nil, errors.Errorf("invalid primary key column %q", option.PrimaryKey)
			}
			pkCols = append(pkCols, index)

		default:
			panic(repr.String(option))
		}
	}
	if err := e.createSource(schemaName, src.Name, colNames, colTypes, pkCols, nil, seqGenerator, persist); err != nil {
		return nil, errors.WithStack(err)
	}
	return exec.Empty, nil
}

// DDL statements such as create materialized view, create source etc need to broadcast to every node in the cluster
// so they can be installed in memory on each node, and we also need to ensure that all statements are executed in
// the exact same order on each node irrespective of where the command originated from.
// In order to do this, we first broadcast the command across the cluster via a raft group which ensures a global
// ordering and we don't process the command locally until we receive it from the cluster.
func (e *Executor) executeInGlobalOrder(schemaName string, sql string, kind string, sequences []uint64) (exec.PullExecutor, error) {
	nextSeq := atomic.AddInt64(&e.ddlStatementSeq, 1)
	statementInfo := &notifications.DDLStatementInfo{
		OriginatingNodeId: int64(e.cluster.GetNodeID()),
		Sequence:          nextSeq,
		SchemaName:        schemaName,
		Sql:               sql,
		TableSequences:    sequences,
		Kind:              kind,
	}
	ch := make(chan statementExecutionResult)
	e.notifActions[nextSeq] = ch

	go func() {
		err := e.cluster.BroadcastNotification(statementInfo)
		if err != nil {
			ch <- statementExecutionResult{err: err}
		}
	}()

	res, ok := <-ch
	if !ok {
		panic("channel was closed")
	}
	return res.exec, res.err
}

func (e *Executor) HandleNotification(notification cluster.Notification) {
	e.ddlExecLock.Lock()
	defer e.ddlExecLock.Unlock()
	ddlStmt := notification.(*notifications.DDLStatementInfo) // nolint: forcetypeassert
	seqGenerator := &preallocSeqGen{sequences: ddlStmt.TableSequences}
	schema := e.metaController.GetOrCreateSchema(ddlStmt.SchemaName)
	session := sess.NewSession(schema, e.ddlExecPlanner)
	if ddlStmt.OriginatingNodeId == int64(e.cluster.GetNodeID()) {
		ch, ok := e.notifActions[ddlStmt.Sequence]
		if !ok {
			panic("cannot find notification")
		}
		delete(e.notifActions, ddlStmt.Sequence)
		if ddlStmt.Kind == "mv" {
			ex, err := e.executeSQLStatementInternal(session, ddlStmt.Sql, false, seqGenerator)
			res := statementExecutionResult{
				exec: ex,
				err:  err,
			}
			ch <- res
		} else {
			ch <- statementExecutionResult{
				exec: exec.Empty,
				err:  nil,
			}
		}
	} else {
		_, err := e.executeSQLStatementInternal(session, ddlStmt.Sql, false, seqGenerator)
		if err != nil {
			log.Printf("Failed to execute broadcast DDL %s for %s %v", ddlStmt.Sql, ddlStmt.SchemaName, err)
		}
	}
}

type statementExecutionResult struct {
	exec exec.PullExecutor
	err  error
}
