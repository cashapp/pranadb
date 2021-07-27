package command

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/squareup/pranadb/sess"

	"github.com/alecthomas/repr"
	"github.com/squareup/pranadb/errors"

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
	cluster           cluster.Cluster
	metaController    *meta.Controller
	pushEngine        *push.PushEngine
	pullEngine        *pull.PullEngine
	ddlStatementSeq   int64
	notifActions      map[int64]chan statementExecutionResult
	ddlExecLock       sync.Mutex
	sessionIDSequence int64
}

func NewCommandExecutor(
	metaController *meta.Controller,
	pushEngine *push.PushEngine,
	pullEngine *pull.PullEngine,
	cluster cluster.Cluster,
) *Executor {
	return &Executor{
		cluster:           cluster,
		metaController:    metaController,
		pushEngine:        pushEngine,
		pullEngine:        pullEngine,
		notifActions:      make(map[int64]chan statementExecutionResult),
		sessionIDSequence: -1,
	}
}

// ExecuteSQLStatement executes a synchronous SQL statement.
func (e *Executor) ExecuteSQLStatement(session *sess.Session, sql string) (exec.PullExecutor, error) {
	// Sessions cannot be accessed concurrently and we also need a memory barrier even if they're not accessed
	// concurrently
	session.Lock.Lock()
	defer session.Lock.Unlock()
	return e.executeSQLStatementInternal(session, sql, true, nil)
}

func (e *Executor) CreateSession(schemaName string) *sess.Session {
	schema := e.metaController.GetOrCreateSchema(schemaName)
	seq := atomic.AddInt64(&e.sessionIDSequence, 1)
	sessionID := fmt.Sprintf("%d-%d", e.cluster.GetNodeID(), seq)
	return sess.NewSession(sessionID, schema, &sessCloser{clus: e.cluster})
}

type sessCloser struct {
	clus cluster.Cluster
}

func (s sessCloser) CloseSession(sessionID string) error {
	shardIDs := s.clus.GetAllShardIDs()
	for _, shardID := range shardIDs {
		sessID := fmt.Sprintf("%s-%d", sessionID, shardID)
		sessCloseMsg := &notifications.SessionClosedMessage{SessionId: sessID}
		err := s.clus.BroadcastNotification(sessCloseMsg)
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *Executor) createSource(schemaName string, name string, colNames []string, colTypes []common.ColumnType, pkCols []int, topicInfo *common.TopicInfo, seqGenerator common.SeqGenerator, persist bool) error {
	log.Printf("creating source %s on node %d", name, e.cluster.GetNodeID())
	id := seqGenerator.GenerateSequence()

	tableInfo := common.TableInfo{
		ID:             id,
		SchemaName:     schemaName,
		Name:           name,
		PrimaryKeyCols: pkCols,
		ColumnNames:    colNames,
		ColumnTypes:    colTypes,
		IndexInfos:     nil,
	}
	sourceInfo := common.SourceInfo{
		TableInfo: &tableInfo,
		TopicInfo: topicInfo,
	}
	err := e.metaController.RegisterSource(&sourceInfo, persist)
	if err != nil {
		return err
	}
	return e.pushEngine.CreateSource(&sourceInfo)
}

func (e *Executor) createMaterializedView(session *sess.Session, name string, query string, seqGenerator common.SeqGenerator, persist bool) error {
	log.Printf("creating mv %s on node %d", name, e.cluster.GetNodeID())
	id := seqGenerator.GenerateSequence()
	mvInfo, err := e.pushEngine.CreateMaterializedView(session.PushPlanner(), session.Schema, name, query, id, seqGenerator)
	if err != nil {
		return err
	}
	err = e.metaController.RegisterMaterializedView(mvInfo, persist)
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
		return nil, errors.MaybeAddStack(err)
	}
	switch {
	case ast.Select != "":
		return e.execSelect(session, ast.Select)
	case ast.Prepare != "":
		return e.execPrepare(session, ast.Prepare)
	case ast.Execute != "":
		return e.execExecute(session, ast.Execute)
	case ast.Drop != "":
		if local {
			sequences, err := e.generateSequences(0)
			if err != nil {
				return nil, err
			}
			_, err = e.execDrop(session, ast.Drop, true)
			if err != nil {
				return nil, err
			}
			return e.executeInGlobalOrder(session.Schema.Name, sql, "drop", sequences)
		}
		return e.execDrop(session, ast.Drop, false)

	case ast.Create != nil && ast.Create.MaterializedView != nil:
		if local {
			// Materialized view needs 2 sequences
			sequences, err := e.generateSequences(2)
			if err != nil {
				return nil, err
			}
			if _, err = e.execCreateMaterializedView(session, ast.Create.MaterializedView, common.NewPreallocSeqGen(sequences), true); err != nil {
				return nil, err
			}
			return e.executeInGlobalOrder(session.Schema.Name, sql, "mv", sequences)
		}
		return e.execCreateMaterializedView(session, ast.Create.MaterializedView, seqGenerator, false)

	case ast.Create != nil && ast.Create.Source != nil:
		if local {
			// Source needs 1 sequence
			sequences, err := e.generateSequences(1)
			if err != nil {
				return nil, err
			}
			_, err = e.execCreateSource(session.Schema.Name, ast.Create.Source, common.NewPreallocSeqGen(sequences), true)
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

func (e *Executor) execPrepare(session *sess.Session, sql string) (exec.PullExecutor, error) {
	// TODO we should really use the parser to do this
	sql = strings.ToLower(sql)
	if strings.Index(sql, "prepare ") != 0 {
		return nil, errors.MaybeAddStack(fmt.Errorf("in valid prepare command %s", sql))
	}
	sql = sql[8:]
	dag, err := e.pullEngine.PrepareSQLStatement(session, sql)
	return dag, errors.MaybeAddStack(err)
}

func (e *Executor) execExecute(session *sess.Session, sql string) (exec.PullExecutor, error) {
	// TODO we should really use the parser to do this
	sql = strings.ToLower(sql)
	if strings.Index(sql, "execute ") != 0 {
		return nil, errors.MaybeAddStack(fmt.Errorf("invalid execute command %s", sql))
	}
	sql = sql[8:]
	// The rest of the command should be a space separated list tokens, the first one is the prepared statement id
	// The others are the args of the prepared statement
	parts := strings.Split(sql, " ")
	if len(parts) == 0 {
		return nil, errors.MaybeAddStack(fmt.Errorf("invalid execute command %s no prepared statement id", sql))
	}
	psID, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return nil, errors.MaybeAddStack(err)
	}
	args := make([]interface{}, len(parts)-1)
	for i := 1; i < len(parts); i++ {
		args[i-1] = parts[i]
	}
	dag, err := e.pullEngine.ExecutePreparedStatement(session, psID, args)
	return dag, errors.MaybeAddStack(err)
}

func (e *Executor) execDrop(session *sess.Session, sql string, persist bool) (exec.PullExecutor, error) {
	// TODO we should really use the parser to do this
	sqlOrig := sql
	sql = strings.ToLower(sql)
	if strings.Index(sql, "drop ") != 0 {
		return nil, errors.MaybeAddStack(fmt.Errorf("invalid drop command %s", sqlOrig))
	}
	sql = sql[5:]
	if strings.HasPrefix(sql, "source ") {
		sourceName := sql[7:]
		if sourceName == "" {
			return nil, errors.MaybeAddStack(fmt.Errorf("invalid drop source command %s no source name specified", sqlOrig))
		}
		sourceInfo, ok := e.metaController.GetSource(session.Schema.Name, sourceName)
		if !ok {
			return nil, errors.MaybeAddStack(fmt.Errorf("source not found %s", sourceName))
		}
		err := e.metaController.RemoveSource(session.Schema.Name, sourceName, persist)
		if err != nil {
			return nil, err
		}
		err = e.pushEngine.RemoveSource(sourceInfo.TableInfo.ID, persist)
		if err != nil {
			return nil, err
		}
	} else if strings.HasPrefix(sql, "materialized view ") {
		mvName := sql[18:]
		if mvName == "" {
			return nil, errors.MaybeAddStack(fmt.Errorf("invalid drop materialized view command %s no materialized view name specified", sqlOrig))
		}
		mvInfo, ok := e.metaController.GetMaterializedView(session.Schema.Name, mvName)
		if !ok {
			return nil, errors.MaybeAddStack(fmt.Errorf("materialized view not found %s", mvName))
		}
		err := e.metaController.RemoveMaterializedView(session.Schema.Name, mvName, persist)
		if err != nil {
			return nil, err
		}
		err = e.pushEngine.RemoveMV(session.Schema, mvInfo, persist)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, errors.MaybeAddStack(fmt.Errorf("invalid drop command %s must be one of drop source <source_name> or drop materialized view <materialized_view_name>", sqlOrig))
	}
	return exec.Empty, nil
}

func (e *Executor) execSelect(session *sess.Session, sql string) (exec.PullExecutor, error) {
	dag, err := e.pullEngine.BuildPullQuery(session, sql)
	return dag, errors.MaybeAddStack(err)
}

func (e *Executor) execCreateMaterializedView(session *sess.Session, mv *parser.CreateMaterializedView, seqGenerator common.SeqGenerator, persist bool) (exec.PullExecutor, error) {
	err := e.createMaterializedView(session, mv.Name.String(), mv.Query.String(), seqGenerator, persist)
	return exec.Empty, errors.MaybeAddStack(err)
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
				return nil, errors.MaybeAddStack(err)
			}
			colTypes = append(colTypes, colType)

		case option.PrimaryKey != "":
			index, ok := colIndex[option.PrimaryKey]
			if !ok {
				return nil, fmt.Errorf("invalid primary key column %q", option.PrimaryKey)
			}
			pkCols = append(pkCols, index)

		default:
			panic(repr.String(option))
		}
	}
	if err := e.createSource(schemaName, src.Name, colNames, colTypes, pkCols, nil, seqGenerator, persist); err != nil {
		return nil, errors.MaybeAddStack(err)
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
	seqGenerator := common.NewPreallocSeqGen(ddlStmt.TableSequences)
	session := e.CreateSession(ddlStmt.SchemaName)
	origNodeID := int(ddlStmt.OriginatingNodeId)
	if origNodeID == e.cluster.GetNodeID() {
		ch, ok := e.notifActions[ddlStmt.Sequence]
		if !ok {
			panic("cannot find notification")
		}
		delete(e.notifActions, ddlStmt.Sequence)
		ch <- statementExecutionResult{
			exec: exec.Empty,
			err:  nil,
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
