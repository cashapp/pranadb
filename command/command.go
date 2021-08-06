package command

import (
	"fmt"

	"github.com/squareup/pranadb/notifier"

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
	notifClient       notifier.Client
	ddlExecLock       sync.Mutex
	sessionIDSequence int64
}

func NewCommandExecutor(
	metaController *meta.Controller,
	pushEngine *push.PushEngine,
	pullEngine *pull.PullEngine,
	cluster cluster.Cluster,
	notifClient notifier.Client,
) *Executor {
	return &Executor{
		cluster:           cluster,
		metaController:    metaController,
		pushEngine:        pushEngine,
		pullEngine:        pullEngine,
		notifClient:       notifClient,
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
		err := s.notifClient.BroadcastNotification(sessCloseMsg)
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *Executor) createSource(schemaName string, name string, colNames []string, colTypes []common.ColumnType,
	pkCols []int, topicInfo *common.TopicInfo, seqGenerator common.SeqGenerator, persist bool) error {

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

//nolint:gocyclo
func (e *Executor) executeSQLStatementInternal(session *sess.Session, sql string, persist bool,
	seqGenerator common.SeqGenerator) (exec.PullExecutor, error) {
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
		exec, err := e.execDrop(session, ast.Drop, persist)
		if err != nil {
			return nil, err
		}
		if persist {
			if err := e.broadcastDDL(session.Schema.Name, sql, nil); err != nil {
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
			if err := e.broadcastDDL(session.Schema.Name, sql, sequences); err != nil {
				return nil, err
			}
		}
		return exec, nil

	case ast.Create != nil && ast.Create.Source != nil:
		var sequences []uint64
		if persist {
			// Source needs 1 sequence
			sequences, err = e.generateSequences(1)
			if err != nil {
				return nil, err
			}
			seqGenerator = common.NewPreallocSeqGen(sequences)
		}
		exec, err := e.execCreateSource(session.Schema.Name, ast.Create.Source, seqGenerator, persist)
		if err != nil {
			return nil, err
		}
		if persist {
			if err := e.broadcastDDL(session.Schema.Name, sql, sequences); err != nil {
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
		// TODO Until we implement proper DDL syncing we need to remove the data from storage before removing from meta controller
		// otherwise SQLTest will think ddl is synced before data is deleted
		err := e.pushEngine.RemoveSource(sourceInfo.TableInfo.ID, persist)
		if err != nil {
			return nil, err
		}
		err = e.metaController.RemoveSource(session.Schema.Name, sourceName, persist)
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

	keyEncoding := common.KafkaEncodingFromString(stripQuotes(src.TopicInformation.KeyEncoding))
	if keyEncoding == common.EncodingUnknown {
		return nil, errors.NewUserErrorF(errors.UnknownTopicEncoding, "Unknown topic encoding %s", src.TopicInformation.KeyEncoding)
	}
	valueEncoding := common.KafkaEncodingFromString(stripQuotes(src.TopicInformation.ValueEncoding))
	if valueEncoding == common.EncodingUnknown {
		return nil, errors.NewUserErrorF(errors.UnknownTopicEncoding, "Unknown topic encoding %s", src.TopicInformation.ValueEncoding)
	}
	props := src.TopicInformation.Properties
	propsMap := make(map[string]string, len(props))
	for _, prop := range props {
		propsMap[stripQuotes(prop.Key)] = stripQuotes(prop.Value)
	}

	cs := src.TopicInformation.ColSelectors
	colSelectors := make([]string, len(cs))
	for i := 0; i < len(cs); i++ {
		colSelectors[i] = stripQuotes(cs[i].Selector)
	}
	lc := len(colSelectors)
	if lc > 0 && lc != len(colTypes) {
		return nil, errors.NewUserErrorF(errors.WrongNumberColumnSelectors,
			"if specified, number of column selectors (%d) must match number of columns (%d)", lc, len(colTypes))
	}

	topicInfo := &common.TopicInfo{
		BrokerName:    stripQuotes(src.TopicInformation.BrokerName),
		TopicName:     stripQuotes(src.TopicInformation.TopicName),
		KeyEncoding:   keyEncoding,
		ValueEncoding: valueEncoding,
		ColSelectors:  colSelectors,
		Properties:    propsMap,
	}
	if err := e.createSource(schemaName, src.Name, colNames, colTypes, pkCols, topicInfo, seqGenerator, persist); err != nil {
		return nil, errors.MaybeAddStack(err)
	}
	return exec.Empty, nil
}

func (e *Executor) broadcastDDL(schemaName string, sql string, sequences []uint64) error {
	statementInfo := &notifications.DDLStatementInfo{
		OriginatingNodeId: int64(e.cluster.GetNodeID()),
		SchemaName:        schemaName,
		Sql:               sql,
		TableSequences:    sequences,
	}
	return e.notifClient.BroadcastNotification(statementInfo)
}

func (e *Executor) HandleNotification(notification notifier.Notification) {
	e.ddlExecLock.Lock()
	defer e.ddlExecLock.Unlock()
	ddlStmt := notification.(*notifications.DDLStatementInfo) // nolint: forcetypeassert
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

func stripQuotes(str string) string {
	if str[0] == '"' && str[len(str)-1] == '"' {
		return str[1 : len(str)-1]
	}
	return str
}
