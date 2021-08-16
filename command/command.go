package command

import (
	"fmt"
	"github.com/squareup/pranadb/notifier"
	"github.com/squareup/pranadb/sess"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/squareup/pranadb/errors"

	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/command/parser"
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
	sessionIDSequence int64
	ddlRunner         *DDLCommandRunner
}

type sessCloser struct {
	clus        cluster.Cluster
	notifClient notifier.Client
}

func NewCommandExecutor(
	metaController *meta.Controller,
	pushEngine *push.PushEngine,
	pullEngine *pull.PullEngine,
	cluster cluster.Cluster,
	notifClient notifier.Client,
) *Executor {
	ex := &Executor{
		cluster:           cluster,
		metaController:    metaController,
		pushEngine:        pushEngine,
		pullEngine:        pullEngine,
		notifClient:       notifClient,
		sessionIDSequence: -1,
	}
	commandRunner := NewDDLCommandRunner(ex)
	ex.ddlRunner = commandRunner
	return ex
}

func (e *Executor) HandleNotification(notification notifier.Notification) {
	e.ddlRunner.HandleNotification(notification)
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
	ast, err := parser.Parse(sql)
	if err != nil {
		return nil, errors.MaybeAddStack(err)
	}

	switch {
	case ast.Select != "":
		dag, err := e.pullEngine.BuildPullQuery(session, sql)
		return dag, errors.MaybeAddStack(err)
	case ast.Prepare != "":
		return e.execPrepare(session, ast.Prepare)
	case ast.Execute != "":
		return e.execExecute(session, ast.Execute)
	case ast.Create != nil && ast.Create.Source != nil:
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
	case ast.Create != nil && ast.Create.MaterializedView != nil:
		sequences, err := e.generateSequences(2)
		if err != nil {
			return nil, err
		}
		command := NewOriginatingCreateMVCommand(e, session.PushPlanner(), session.Schema, sql, sequences, ast.Create.MaterializedView)
		err = e.ddlRunner.RunCommand(command)
		if err != nil {
			return nil, err
		}
		return exec.Empty, nil
	case ast.Drop != "":
		// TODO we should really use the parser to do this
		sql = strings.ToLower(sql)
		space := regexp.MustCompile(`\s+`)
		sql = space.ReplaceAllString(sql, " ")
		sql = strings.TrimLeft(sql, " ")
		if strings.HasPrefix(sql, "drop source ") {
			command := NewDropSourceCommand(e, session.Schema.Name, sql)
			err = e.ddlRunner.RunCommand(command)
			if err != nil {
				return nil, err
			}
			return exec.Empty, nil
		}
		if strings.HasPrefix(sql, "drop materialized view ") {
			command := NewDropMVCommand(e, session.Schema.Name, sql)
			err = e.ddlRunner.RunCommand(command)
			if err != nil {
				return nil, err
			}
			return exec.Empty, nil
		}
	}
	return nil, fmt.Errorf("invalid statement %s", sql)
}

func (e *Executor) CreateSession(schemaName string) *sess.Session {
	schema := e.metaController.GetOrCreateSchema(schemaName)
	seq := atomic.AddInt64(&e.sessionIDSequence, 1)
	sessionID := fmt.Sprintf("%d-%d", e.cluster.GetNodeID(), seq)
	return sess.NewSession(sessionID, schema, &sessCloser{clus: e.cluster, notifClient: e.notifClient})
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

// GetPushEngine is only used in testing
func (e *Executor) GetPushEngine() *push.PushEngine {
	return e.pushEngine
}

func (e *Executor) GetPullEngine() *pull.PullEngine {
	return e.pullEngine
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
