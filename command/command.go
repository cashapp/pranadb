package command

import (
	"fmt"
	"github.com/squareup/pranadb/conf"
	"github.com/squareup/pranadb/protos/squareup/cash/pranadb/v1/clustermsgs"
	"github.com/squareup/pranadb/remoting"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/squareup/pranadb/failinject"
	"github.com/squareup/pranadb/table"

	"github.com/alecthomas/participle/v2"
	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/command/parser"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/execctx"
	"github.com/squareup/pranadb/meta"
	"github.com/squareup/pranadb/protolib"
	"github.com/squareup/pranadb/pull"
	"github.com/squareup/pranadb/pull/exec"
	"github.com/squareup/pranadb/push"
)

const ddlRetryTimeout = 1 * time.Minute

type Executor struct {
	cluster           cluster.Cluster
	metaController    *meta.Controller
	pushEngine        *push.Engine
	pullEngine        *pull.Engine
	protoRegistry     protolib.Resolver
	execCtxIDSequence int64
	ddlRunner         *DDLCommandRunner
	failureInjector   failinject.Injector
	ddlClient         remoting.Broadcaster
	ddlResetClient    remoting.Broadcaster
	config            *conf.Config
}

func NewCommandExecutor(metaController *meta.Controller, pushEngine *push.Engine, pullEngine *pull.Engine,
	cluster cluster.Cluster, ddlClient remoting.Broadcaster, ddlResetClient remoting.Broadcaster, protoRegistry protolib.Resolver,
	failureInjector failinject.Injector, config *conf.Config) *Executor {
	ex := &Executor{
		cluster:           cluster,
		metaController:    metaController,
		pushEngine:        pushEngine,
		pullEngine:        pullEngine,
		protoRegistry:     protoRegistry,
		execCtxIDSequence: -1,
		failureInjector:   failureInjector,
		ddlClient:         ddlClient,
		ddlResetClient:    ddlResetClient,
		config:            config,
	}
	commandRunner := NewDDLCommandRunner(ex)
	ex.ddlRunner = commandRunner
	return ex
}

func (e *Executor) DDlCommandRunner() *DDLCommandRunner {
	return e.ddlRunner
}

func (e *Executor) Start() error {
	return nil
}

func (e *Executor) Stop() error {
	e.ddlClient.Stop()
	e.ddlResetClient.Stop()
	return nil
}

// ExecuteSQLStatement executes a synchronous SQL statement.
//nolint:gocyclo
func (e *Executor) ExecuteSQLStatement(execCtx *execctx.ExecutionContext, sql string, argTypes []common.ColumnType,
	args []interface{}) (exec.PullExecutor, error) {
	ast, err := parser.Parse(sql)
	if err != nil {
		var perr participle.Error
		if errors.As(err, &perr) {
			return nil, errors.NewInvalidStatementError(err.Error())
		}
		return nil, errors.WithStack(err)
	}

	switch {
	case ast.Select != "":
		dag, err := e.pullEngine.BuildPullQuery(execCtx, sql, argTypes, args)
		return dag, errors.WithStack(err)
	case ast.Create != nil && ast.Create.Source != nil:
		sequences, err := e.generateTableIDSequences(1)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		consumerGroupID, err := CreateConsumerGroupID(e.config.ClusterID)
		if err != nil {
			return nil, err
		}
		command := NewOriginatingCreateSourceCommand(e, execCtx.Schema.Name, sql, sequences, ast.Create.Source, consumerGroupID)
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
		command, err := NewOriginatingCreateMVCommand(e, execCtx.Planner(), execCtx.Schema, sql, sequences, ast.Create.MaterializedView)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		if err := e.executeCommandWithRetry(command); err != nil {
			return nil, errors.WithStack(err)
		}
		return exec.Empty, nil
	case ast.Create != nil && ast.Create.Index != nil:
		sequences, err := e.generateTableIDSequences(1)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		command, err := NewOriginatingCreateIndexCommand(e, execCtx.Planner(), execCtx.Schema, sql, sequences, ast.Create.Index)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		if err := e.executeCommandWithRetry(command); err != nil {
			return nil, errors.WithStack(err)
		}
		return exec.Empty, nil
	case ast.Drop != nil && ast.Drop.Source:
		command := NewOriginatingDropSourceCommand(e, execCtx.Schema.Name, sql, ast.Drop.Name)
		err = e.ddlRunner.RunCommand(command)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return exec.Empty, nil
	case ast.Drop != nil && ast.Drop.MaterializedView:
		command := NewOriginatingDropMVCommand(e, execCtx.Schema.Name, sql, ast.Drop.Name)
		err = e.ddlRunner.RunCommand(command)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return exec.Empty, nil
	case ast.Drop != nil && ast.Drop.Index:
		command := NewOriginatingDropIndexCommand(e, execCtx.Schema.Name, sql, ast.Drop.TableName, ast.Drop.Name)
		err = e.ddlRunner.RunCommand(command)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return exec.Empty, nil
	case ast.Show != nil && ast.Show.Tables:
		rows, err := e.execShowTables(execCtx)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return rows, nil
	case ast.Show != nil && ast.Show.Schemas:
		rows, err := e.execShowSchemas()
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return rows, nil
	case ast.Show != nil && ast.Show.Indexes:
		rows, err := e.execShowIndexes(execCtx.Schema.Name, ast.Show.TableName)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return rows, nil
	case ast.Describe != "":
		rows, err := e.execDescribe(execCtx, strings.ToLower(ast.Describe))
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return rows, nil
	case ast.ResetDdl != "":
		if err := e.DDlCommandRunner().Cancel(ast.ResetDdl); err != nil {
			return nil, errors.WithStack(err)
		}
		return exec.Empty, nil
	case ast.ConsumerRate != nil:
		if err := e.execConsumerRate(execCtx, ast.ConsumerRate.SourceName, ast.ConsumerRate.Rate); err != nil {
			return nil, errors.WithStack(err)
		}
		return exec.Empty, nil
	}
	return nil, errors.Errorf("invalid statement %s", sql)
}

func (e *Executor) CreateExecutionContext(schema *common.Schema) *execctx.ExecutionContext {
	seq := atomic.AddInt64(&e.execCtxIDSequence, 1)
	ctxID := fmt.Sprintf("%d-%d", e.cluster.GetNodeID(), seq)
	return execctx.NewExecutionContext(ctxID, schema)
}

// GetPushEngine is only used in testing
func (e *Executor) GetPushEngine() *push.Engine {
	return e.pushEngine
}

func (e *Executor) GetPullEngine() *pull.Engine {
	return e.pullEngine
}

func (e *Executor) executeCommandWithRetry(command DDLCommand) error {
	start := time.Now()
	for {
		err := e.ddlRunner.RunCommand(command)
		if err != nil {
			var perr errors.PranaError
			if errors.As(err, &perr) && perr.Code == errors.DdlRetry {
				// Some DDL commands like create MV or index can return DdlRetry if they fail because Raft
				// leadership changed - in this case we retry rather than returning an error as this can be transient
				// e.g. cluster is starting up or node is being rolled
				time.Sleep(2 * time.Second)
				if time.Now().Sub(start) > ddlRetryTimeout {
					return errors.NewPranaErrorf(errors.Timeout, "timed out in retrying ddl command")
				}
				continue
			}
			return err
		}
		return nil
	}
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

func (e *Executor) execShowTables(execCtx *execctx.ExecutionContext) (exec.PullExecutor, error) {
	rows, err := e.pullEngine.ExecuteQuery("sys", fmt.Sprintf("select name, kind from tables where schema_name='%s' and kind <> 'internal' order by kind, name", execCtx.Schema.Name))
	if err != nil {
		return nil, errors.WithStack(err)
	}

	staticRows, err := exec.NewStaticRows([]string{fmt.Sprintf("tables_in_%s", execCtx.Schema.Name), "table_type"}, rows)
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

func (e *Executor) execShowIndexes(schemaName string, tableName string) (exec.PullExecutor, error) {
	var tableInfo *common.TableInfo
	src, ok := e.metaController.GetSource(schemaName, tableName)
	if !ok {
		mv, ok := e.metaController.GetMaterializedView(schemaName, tableName)
		if !ok {
			return nil, errors.NewPranaErrorf(errors.UnknownTable, "Unknown table %s.%s", schemaName, tableName)
		}
		tableInfo = mv.GetTableInfo()
	} else {
		tableInfo = src.GetTableInfo()
	}
	rowsFactory := common.NewRowsFactory(
		[]common.ColumnType{common.VarcharColumnType, common.VarcharColumnType},
	)
	rows := rowsFactory.NewRows(len(tableInfo.IndexInfos))

	// Sort the index names
	var indNames []string
	for indexName := range tableInfo.IndexInfos {
		indNames = append(indNames, indexName)
	}
	sort.Strings(indNames)

	for _, indexName := range indNames {
		indexInfo := tableInfo.IndexInfos[indexName]
		rows.AppendStringToColumn(0, indexName)
		sb := strings.Builder{}
		for i, index := range indexInfo.IndexCols {
			colName := tableInfo.ColumnNames[index]
			sb.WriteString(colName)
			if i != len(indexInfo.IndexCols)-1 {
				sb.WriteString(", ")
			}
		}
		rows.AppendStringToColumn(1, sb.String())
	}
	staticRows, err := exec.NewStaticRows([]string{fmt.Sprintf("indexes_on_%s", tableName), "columns"}, rows)
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
		if tableInfo.IsPrimaryKeyCol(columnIndex) {
			resultRows.AppendStringToColumn(2, "pri")
		} else {
			resultRows.AppendStringToColumn(2, "")
		}
	}
	staticRows, err := exec.NewStaticRows([]string{"field", "type", "key"}, resultRows)
	return staticRows, errors.WithStack(err)
}

func (e *Executor) execDescribe(execCtx *execctx.ExecutionContext, tableName string) (exec.PullExecutor, error) {
	// NB: We select a specific set of columns because the Decode*Row() methods expect a row with *_info columns on certain predefined positions.
	rows, err := e.pullEngine.ExecuteQuery("sys", fmt.Sprintf("select id, kind, schema_name, name, table_info, topic_info, query, mv_name from tables where schema_name='%s' and name='%s'", execCtx.Schema.Name, tableName))
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if rows.RowCount() == 0 {
		return nil, errors.NewUnknownTableError(execCtx.Schema.Name, tableName)
	}
	if rows.RowCount() != 1 {
		panic(fmt.Sprintf("multiple matches for table: '%s.%s'", execCtx.Schema.Name, tableName))
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

func (e *Executor) execConsumerRate(execCtx *execctx.ExecutionContext, sourceName string, rate int64) error {
	return e.ddlClient.Broadcast(&clustermsgs.ConsumerSetRate{
		SchemaName: execCtx.Schema.Name,
		SourceName: sourceName,
		Rate:       rate,
	})
}

func (e *Executor) Empty() bool {
	return e.ddlRunner.empty()
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
		ConditionalTableID: tableID,
		Prefixes:           prefixes,
	}
	if err := clust.AddToDeleteBatch(batch); err != nil {
		return nil, err
	}
	return batch, nil
}
