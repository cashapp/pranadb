package command

import (
	"fmt"
	"github.com/squareup/pranadb/failinject"
	"github.com/squareup/pranadb/table"
	"strings"
	"sync/atomic"

	log "github.com/sirupsen/logrus"

	"github.com/alecthomas/participle/v2"
	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/command/parser"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/meta"
	"github.com/squareup/pranadb/notifier"
	"github.com/squareup/pranadb/protolib"
	"github.com/squareup/pranadb/protos/squareup/cash/pranadb/v1/notifications"
	"github.com/squareup/pranadb/pull"
	"github.com/squareup/pranadb/pull/exec"
	"github.com/squareup/pranadb/push"
	"github.com/squareup/pranadb/sess"
)

type Executor struct {
	cluster           cluster.Cluster
	metaController    *meta.Controller
	pushEngine        *push.Engine
	pullEngine        *pull.Engine
	notifClient       notifier.Client
	protoRegistry     protolib.Resolver
	sessionIDSequence int64
	ddlRunner         *DDLCommandRunner
	failureInjector   failinject.Injector
}

type sessCloser struct {
	clus        cluster.Cluster
	notifClient notifier.Client
}

func NewCommandExecutor(metaController *meta.Controller, pushEngine *push.Engine, pullEngine *pull.Engine,
	cluster cluster.Cluster, notifClient notifier.Client, protoRegistry protolib.Resolver,
	failureInjector failinject.Injector) *Executor {
	ex := &Executor{
		cluster:           cluster,
		metaController:    metaController,
		pushEngine:        pushEngine,
		pullEngine:        pullEngine,
		notifClient:       notifClient,
		protoRegistry:     protoRegistry,
		sessionIDSequence: -1,
		failureInjector:   failureInjector,
	}
	commandRunner := NewDDLCommandRunner(ex)
	ex.ddlRunner = commandRunner
	return ex
}

func (e *Executor) HandleNotification(notification notifier.Notification) error {
	return e.ddlRunner.HandleNotification(notification)
}

func (e *Executor) Start() error {
	return e.notifClient.Start()
}

func (e *Executor) Stop() error {
	return e.notifClient.Stop()
}

// ExecuteSQLStatement executes a synchronous SQL statement.
//nolint:gocyclo
func (e *Executor) ExecuteSQLStatement(session *sess.Session, sql string) (exec.PullExecutor, error) {
	// Sessions cannot be accessed concurrently and we also need a memory barrier even if they're not accessed
	// concurrently
	session.Lock.Lock()
	defer session.Lock.Unlock()

	log.Printf("Executing sql statement \"%s\"", sql)

	ast, err := parser.Parse(sql)
	if err != nil {
		var perr participle.Error
		if errors.As(err, &perr) {
			return nil, errors.NewInvalidStatementError(err.Error())
		}
		return nil, errors.WithStack(err)
	}

	if session.Schema == nil && ast.Use == "" {
		return nil, errors.NewSchemaNotInUseError()
	}

	if session.Schema != nil && session.Schema.IsDeleted() {
		schema := e.metaController.GetOrCreateSchema(session.Schema.Name)
		session.UseSchema(schema)
	}

	switch {
	case ast.Select != "":
		session.PullPlanner().RefreshInfoSchema()
		dag, err := e.pullEngine.BuildPullQuery(session, sql)
		return dag, errors.WithStack(err)
	case ast.Prepare != "":
		session.PullPlanner().RefreshInfoSchema()
		ex, err := e.execPrepare(session, ast.Prepare)
		return ex, errors.WithStack(err)
	case ast.Execute != nil:
		ex, err := e.execExecute(session, ast.Execute)
		return ex, errors.WithStack(err)
	case ast.Create != nil && ast.Create.Source != nil:
		sequences, err := e.generateTableIDSequences(1)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		command := NewOriginatingCreateSourceCommand(e, session.Schema.Name, sql, sequences, ast.Create.Source)
		err = e.ddlRunner.RunCommand(command)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return exec.Empty, nil
	case ast.Create != nil && ast.Create.MaterializedView != nil:
		sequences, err := e.generateTableIDSequences(3)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		command := NewOriginatingCreateMVCommand(e, session.PushPlanner(), session.Schema, sql, sequences, ast.Create.MaterializedView)
		err = e.ddlRunner.RunCommand(command)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return exec.Empty, nil
	case ast.Create != nil && ast.Create.Index != nil:
		sequences, err := e.generateTableIDSequences(1)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		command := NewOriginatingCreateIndexCommand(e, session.PushPlanner(), session.Schema, sql, sequences, ast.Create.Index)
		err = e.ddlRunner.RunCommand(command)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return exec.Empty, nil
	case ast.Drop != nil && ast.Drop.Source:
		command := NewOriginatingDropSourceCommand(e, session.Schema.Name, sql, ast.Drop.Name)
		err = e.ddlRunner.RunCommand(command)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return exec.Empty, nil
	case ast.Drop != nil && ast.Drop.MaterializedView:
		command := NewOriginatingDropMVCommand(e, session.Schema.Name, sql, ast.Drop.Name)
		err = e.ddlRunner.RunCommand(command)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return exec.Empty, nil
	case ast.Drop != nil && ast.Drop.Index:
		command := NewOriginatingDropIndexCommand(e, session.Schema.Name, sql, ast.Drop.TableName, ast.Drop.Name)
		err = e.ddlRunner.RunCommand(command)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return exec.Empty, nil
	case ast.Use != "":
		return e.execUse(session, ast.Use)
	case ast.Show != nil && ast.Show.Tables != "":
		rows, err := e.execShowTables(session)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return rows, nil
	}
	return nil, errors.Errorf("invalid statement %s", sql)
}

func (e *Executor) CreateSession() *sess.Session {
	seq := atomic.AddInt64(&e.sessionIDSequence, 1)
	sessionID := fmt.Sprintf("%d-%d", e.cluster.GetNodeID(), seq)
	return sess.NewSession(sessionID, &sessCloser{clus: e.cluster, notifClient: e.notifClient})
}

func (s *sessCloser) CloseRemoteSessions(sessionID string) error {
	shardIDs := s.clus.GetAllShardIDs()
	for _, shardID := range shardIDs {
		sessID := fmt.Sprintf("%s-%d", sessionID, shardID)
		sessCloseMsg := &notifications.SessionClosedMessage{SessionId: sessID}
		err := s.notifClient.BroadcastOneway(sessCloseMsg)
		if err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

// GetPushEngine is only used in testing
func (e *Executor) GetPushEngine() *push.Engine {
	return e.pushEngine
}

func (e *Executor) GetPullEngine() *pull.Engine {
	return e.pullEngine
}

func (e *Executor) generateTableIDSequences(numValues int) ([]uint64, error) {
	tableIDSequences := make([]uint64, numValues)
	for i := 0; i < numValues; i++ {
		v, err := e.cluster.GenerateClusterSequence("table")
		if err != nil {
			return nil, errors.WithStack(err)
		}
		tableIDSequences[i] = v + common.UserTableIDBase
	}
	return tableIDSequences, nil
}

func (e *Executor) execPrepare(session *sess.Session, sql string) (exec.PullExecutor, error) {
	// TODO we should really use the parser to do this
	sql = strings.ToLower(sql)
	if strings.Index(sql, "prepare ") != 0 {
		return nil, errors.Errorf("in valid prepare command %s", sql)
	}
	sql = sql[8:]
	return e.pullEngine.PrepareSQLStatement(session, sql)
}

func (e *Executor) execExecute(session *sess.Session, execute *parser.Execute) (exec.PullExecutor, error) {
	session.PullPlanner().RefreshInfoSchema()
	args := make([]interface{}, len(execute.Args))
	for i := range args {
		args[i] = execute.Args[i]
	}
	return e.pullEngine.ExecutePreparedStatement(session, execute.PsID, args)
}

func (e *Executor) execUse(session *sess.Session, schemaName string) (exec.PullExecutor, error) {
	// TODO auth checks
	schema := e.metaController.GetOrCreateSchema(schemaName)
	session.UseSchema(schema)
	return exec.Empty, nil
}

func (e *Executor) execShowTables(session *sess.Session) (exec.PullExecutor, error) {
	rows, err := e.pullEngine.ExecuteQuery("sys", fmt.Sprintf("select name, kind from tables where schema_name='%s' and kind <> 'internal' order by kind, name", session.Schema.Name))
	if err != nil {
		return nil, errors.WithStack(err)
	}

	staticRows, err := exec.NewStaticRows([]string{"table", "kind"}, rows)
	return staticRows, errors.WithStack(err)
}

func (e *Executor) RunningCommands() int {
	return e.ddlRunner.runningCommands()
}

func (e *Executor) FailureInjector() failinject.Injector {
	return e.failureInjector
}

func storeToDeletePrefixes(tableID uint64, clust cluster.Cluster) ([][]byte, error) {
	// We record prefixes in the to_delete table - this makes sure MV data is deleted on restart if failure occurs
	// after this
	var prefixes [][]byte
	for _, shardID := range clust.GetAllShardIDs() {
		prefix := table.EncodeTableKeyPrefix(tableID, shardID, 16)
		prefixes = append(prefixes, prefix)
	}
	if err := clust.AddPrefixesToDelete(false, prefixes...); err != nil {
		return nil, err
	}
	return prefixes, nil
}
