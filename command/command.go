package command

import (
	"fmt"
	"github.com/squareup/pranadb/notifier"
	"github.com/squareup/pranadb/sess"
	"log"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	
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
	notifClient       notifier.Client
	ddlExecLock       sync.Mutex
	sessionIDSequence int64
	ddlRunner         *DDLCommandRunner
}

func NewCommandExecutor(
	metaController *meta.Controller,
	pushEngine *push.PushEngine,
	pullEngine *pull.PullEngine,
	cluster cluster.Cluster,
	notifClient notifier.Client,
) *Executor {
	exec := &Executor{
		cluster:           cluster,
		metaController:    metaController,
		pushEngine:        pushEngine,
		pullEngine:        pullEngine,
		notifClient:       notifClient,
		sessionIDSequence: -1,
	}
	commandRunner := NewDDLCommandRunner(exec)
	exec.ddlRunner = commandRunner
	return exec
}

func (e *Executor) Start() error {
	return e.notifClient.Start()
}

func (e *Executor) Stop() error {
	return e.notifClient.Stop()
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
	return sess.NewSession(sessionID, schema, &sessCloser{clus: e.cluster, notifClient: e.notifClient})
}

type sessCloser struct {
	clus        cluster.Cluster
	notifClient notifier.Client
}

func (s *sessCloser) CloseRemoteSessions(sessionID string) error {
	shardIDs := s.clus.GetAllShardIDs()
	for _, shardID := range shardIDs {
		sessID := fmt.Sprintf("%s-%d", sessionID, shardID)
		sessCloseMsg := &notifications.SessionClosedMessage{SessionId: sessID}
		err := s.notifClient.BroadcastOneway(sessCloseMsg)
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *Executor) createMaterializedView(session *sess.Session, name string, query string, seqGenerator common.SeqGenerator, persist bool) error {
	id := seqGenerator.GenerateSequence()
	// FIXME - this needs to be in the opposite order
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

func (e *Executor) GetPullEngine() *pull.PullEngine {
	return e.pullEngine
}

//nolint:gocyclo
func (e *Executor) executeSQLStatementInternal(session *sess.Session, sql string, persist bool,
	seqGenerator common.SeqGenerator) (exec.PullExecutor, error) {
	ast, err := parser.Parse(sql)
	if err != nil {
		return nil, errors.MaybeAddStack(err)
	}

	if ast.Create != nil && ast.Create.Source != nil {
		sequences, err := e.generateSequences(1)
		if err != nil {
			return nil, err
		}
		command := NewOriginatingCreateSourceCommand(e, session.Schema.Name, sql, sequences, ast.Create.Source)
		err = e.ddlRunner.RunCommand(command)
		if err != nil {
			return nil, err
		}
		return exec.Empty, nil
	}

	switch {
	case ast.Select != "":
		return e.execSelect(session, ast.Select)
	case ast.Prepare != "":
		return e.execPrepare(session, ast.Prepare)
	case ast.Execute != "":
		return e.execExecute(session, ast.Execute)
	case ast.Drop != nil:
		exec, err := e.execDrop(session, ast.Drop, persist)
		if err != nil {
			return nil, err
		}
		if persist {
			if err := e.broadcastDDL(session.Schema.Name, sql, nil, DDLCommandTypeDropMV); err != nil {
				return nil, err
			}
		}
		return exec, nil
	case ast.Create != nil && ast.Create.MaterializedView != nil:
		var sequences []uint64
		if persist {
			// Materialized view needs 2 sequences
			sequences, err = e.generateSequences(2)
			if err != nil {
				return nil, err
			}
			seqGenerator = common.NewPreallocSeqGen(sequences)
		}
		exec, err := e.execCreateMaterializedView(session, ast.Create.MaterializedView, seqGenerator, persist)
		if err != nil {
			return nil, err
		}
		if persist {
			if err := e.broadcastDDL(session.Schema.Name, sql, sequences, DDLCommandTypeCreateMV); err != nil {
				return nil, err
			}
		}
		return exec, nil
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

func (e *Executor) execDrop(session *sess.Session, drop *parser.Drop, persist bool) (exec.PullExecutor, error) {
	if drop.Source {
		sourceName := drop.Name
		sourceInfo, ok := e.metaController.GetSource(session.Schema.Name, sourceName)
		if !ok {
			return nil, errors.MaybeAddStack(fmt.Errorf("source not found %s", sourceName))
		}
		// TODO Until we implement proper DDL syncing we need to remove the data from storage before removing from meta controller
		// otherwise SQLTest will think ddl is synced before data is deleted
		err := e.pushEngine.RemoveSource(sourceInfo, persist)
		if err != nil {
			return nil, err
		}
		err = e.metaController.RemoveSource(session.Schema.Name, sourceName, persist)
		if err != nil {
			return nil, err
		}
	} else if drop.MaterializedView {
		mvName := drop.Name
		mvInfo, ok := e.metaController.GetMaterializedView(session.Schema.Name, mvName)
		if !ok {
			return nil, errors.MaybeAddStack(fmt.Errorf("materialized view not found %s", mvName))
		}
		// TODO Until we implement proper DDL syncing we need to remove the data from storage before removing from meta controller
		// otherwise SQLTest will think ddl is synced before data is deleted
		err := e.pushEngine.RemoveMV(session.Schema, mvInfo, persist)
		if err != nil {
			return nil, err
		}
		err = e.metaController.RemoveMaterializedView(session.Schema.Name, mvName, persist)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, errors.MaybeAddStack(fmt.Errorf("invalid drop command"))
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

func (e *Executor) broadcastDDL(schemaName string, sql string, sequences []uint64, commandType DDLCommandType) error {
	statementInfo := &notifications.DDLStatementInfo{
		OriginatingNodeId: int64(e.cluster.GetNodeID()),
		SchemaName:        schemaName,
		Sql:               sql,
		TableSequences:    sequences,
		CommandType:       int32(commandType),
	}
	return e.notifClient.BroadcastOneway(statementInfo)
}

func (e *Executor) HandleNotification(notification notifier.Notification) {
	e.ddlExecLock.Lock()
	defer e.ddlExecLock.Unlock()

	ddlStmt := notification.(*notifications.DDLStatementInfo) // nolint: forcetypeassert

	if ddlStmt.CommandType == DDLCommandTypeCreateSource {
		e.ddlRunner.HandleNotification(notification)
		return
	}

	seqGenerator := common.NewPreallocSeqGen(ddlStmt.TableSequences)
	// Note this session does not need to be closed as it does not execute any pull queries
	session := e.CreateSession(ddlStmt.SchemaName)
	origNodeID := int(ddlStmt.OriginatingNodeId)
	if origNodeID != e.cluster.GetNodeID() {
		_, err := e.executeSQLStatementInternal(session, ddlStmt.Sql, false, seqGenerator)
		if err != nil {
			log.Printf("Failed to execute broadcast DDL %s for %s %v", ddlStmt.Sql, ddlStmt.SchemaName, err)
		}
	}
}
