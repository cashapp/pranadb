package command

import (
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/squareup/pranadb/failinject"
	"github.com/squareup/pranadb/table"

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
	e.ddlRunner.clear()
	return e.notifClient.Stop()
}

// ExecuteSQLStatement executes a synchronous SQL statement.
//nolint:gocyclo
func (e *Executor) ExecuteSQLStatement(session *sess.Session, sql string) (exec.PullExecutor, error) {
	// Sessions cannot be accessed concurrently and we also need a memory barrier even if they're not accessed
	// concurrently
	session.Lock.Lock()
	defer session.Lock.Unlock()

	ast, err := parser.Parse(sql)
	if err != nil {
		var perr participle.Error
		if errors.As(err, &perr) {
			return nil, errors.NewInvalidStatementError(err.Error())
		}
		return nil, errors.WithStack(err)
	}

	if session.Schema == nil && isSchemaNeeded(ast) {
		return nil, errors.NewSchemaNotInUseError()
	}

	if session.Schema != nil && session.Schema.IsDeleted() && isSchemaNeeded(ast) {
		schema := e.metaController.GetOrCreateSchema(session.Schema.Name)
		session.UseSchema(schema)
	}

	switch {
	case ast.Select != "":
		session.Planner().RefreshInfoSchema()
		dag, err := e.pullEngine.BuildPullQuery(session, sql)
		return dag, errors.WithStack(err)
	case ast.Prepare != "":
		session.Planner().RefreshInfoSchema()
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
		command := NewOriginatingCreateMVCommand(e, session.Planner(), session.Schema, sql, sequences, ast.Create.MaterializedView)
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
		command := NewOriginatingCreateIndexCommand(e, session.Planner(), session.Schema, sql, sequences, ast.Create.Index)
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
	case ast.Show != nil && ast.Show.Schemas != "":
		rows, err := e.execShowSchemas()
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return rows, nil
	case ast.Describe != "":
		rows, err := e.execDescribe(session, ast.Describe)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return rows, nil
	}
	return nil, errors.Errorf("invalid statement %s", sql)
}

// checks if it's necessary to use a schema namespace to run the SQL statement
func isSchemaNeeded(ast *parser.AST) bool {
	switch {
	case ast.Use != "":
		return false
	case ast.Show != nil && ast.Show.Schemas != "":
		return false
	}
	return true
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
	session.Planner().RefreshInfoSchema()
	args := make([]interface{}, len(execute.Args))
	for i := range args {
		args[i] = execute.Args[i]
	}
	return e.pullEngine.ExecutePreparedStatement(session, execute.PsID, args)
}

func (e *Executor) execUse(session *sess.Session, schemaName string) (exec.PullExecutor, error) {
	// TODO auth checks
	previousSchema := session.Schema
	schema := e.metaController.GetOrCreateSchema(schemaName)
	session.UseSchema(schema)
	// delete previousSchema if empty after switching to new schema
	if previousSchema != nil && previousSchema.Name != schemaName {
		e.metaController.DeleteSchemaIfEmpty(previousSchema)
	}
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

func (e *Executor) execShowSchemas() (exec.PullExecutor, error) {
	schemaNames := e.metaController.GetSchemaNames()
	rowsFactory := common.NewRowsFactory(
		[]common.ColumnType{common.VarcharColumnType},
	)
	rows := rowsFactory.NewRows(len(schemaNames))
	for _, schemaName := range schemaNames {
		rows.AppendStringToColumn(0, schemaName)
	}
	staticRows, err := exec.NewStaticRows([]string{"schema"}, rows)
	return staticRows, errors.WithStack(err)
}

var describeRowsFactory = common.NewRowsFactory(
	[]common.ColumnType{
		{Type: common.TypeVarchar}, // field
		{Type: common.TypeVarchar}, // type
		{Type: common.TypeVarchar}, // key
	},
)

func describeRows(tableInfo *common.TableInfo) (exec.PullExecutor, error) {
	resultRows := describeRowsFactory.NewRows(len(tableInfo.ColumnNames))
	for columnIndex, columnName := range tableInfo.ColumnNames {
		if tableInfo.ColsVisible != nil && !tableInfo.ColsVisible[columnIndex] {
			continue
		}
		resultRows.AppendStringToColumn(0, columnName)
		resultRows.AppendStringToColumn(1, tableInfo.ColumnTypes[columnIndex].String())
		if tableInfo.IsPrimaryKey(columnIndex) {
			resultRows.AppendStringToColumn(2, "pk")
		} else {
			resultRows.AppendStringToColumn(2, "")
		}
	}
	staticRows, err := exec.NewStaticRows([]string{"field", "type", "key"}, resultRows)
	return staticRows, errors.WithStack(err)
}

func (e *Executor) execDescribe(session *sess.Session, tableName string) (exec.PullExecutor, error) {
	// NB: We select a specific set of columns because the Decode*Row() methods expect a row with *_info columns on certain predefined positions.
	rows, err := e.pullEngine.ExecuteQuery("sys", fmt.Sprintf("select id, kind, schema_name, name, table_info, topic_info, query, mv_name from tables where schema_name='%s' and name='%s'", session.Schema.Name, tableName))
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if rows.RowCount() == 0 {
		return nil, errors.NewUnknownSourceOrMaterializedViewError(session.Schema.Name, tableName)
	}
	if rows.RowCount() != 1 {
		panic(fmt.Sprintf("multiple matches for table: '%s.%s'", session.Schema.Name, tableName))
	}
	tableRow := rows.GetRow(0)
	var tableInfo *common.TableInfo
	kind := tableRow.GetString(1)
	switch kind {
	case meta.TableKindSource:
		tableInfo = meta.DecodeSourceInfoRow(&tableRow).TableInfo
	case meta.TableKindMaterializedView:
		tableInfo = meta.DecodeMaterializedViewInfoRow(&tableRow).TableInfo
	case meta.TableKindInternal:
		// NB: This case is for completness as sys.table doesn't know about internal tables.
		tableInfo = meta.DecodeInternalTableInfoRow(&tableRow).TableInfo
	}
	if tableInfo == nil {
		panic(fmt.Sprintf("unknown table kind: '%s'", kind))
	}
	return describeRows(tableInfo)
}

func (e *Executor) RunningCommands() int {
	return e.ddlRunner.runningCommands()
}

func (e *Executor) FailureInjector() failinject.Injector {
	return e.failureInjector
}

func storeToDeleteBatch(tableID uint64, clust cluster.Cluster) (*cluster.ToDeleteBatch, error) {
	// We record prefixes in the to_delete table - this makes sure data is deleted on restart if failure occurs
	// after this
	var prefixes [][]byte
	for _, shardID := range clust.GetAllShardIDs() {
		prefix := table.EncodeTableKeyPrefix(tableID, shardID, 16)
		prefixes = append(prefixes, prefix)
	}
	batch := &cluster.ToDeleteBatch{
		Local:              false,
		ConditionalTableID: tableID,
		Prefixes:           prefixes,
	}
	if err := clust.AddToDeleteBatch(batch); err != nil {
		return nil, err
	}
	return batch, nil
}
