package pull

import (
	"context"
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/sharder"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/execctx"
	"github.com/squareup/pranadb/meta"
	"github.com/squareup/pranadb/pull/exec"
)

type Engine struct {
	lock              sync.RWMutex
	started           bool
	queryExecCtxCache atomic.Value
	cluster           cluster.Cluster
	metaController    *meta.Controller
	nodeID            int
	shrder            *sharder.Sharder
	available         common.AtomicBool
}

func NewPullEngine(cluster cluster.Cluster, metaController *meta.Controller, shrder *sharder.Sharder) *Engine {
	engine := Engine{
		cluster:        cluster,
		metaController: metaController,
		nodeID:         cluster.GetNodeID(),
		shrder:         shrder,
	}
	engine.queryExecCtxCache.Store(new(sync.Map))
	return &engine
}

func (p *Engine) Start() error {
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.started {
		return nil
	}
	p.started = true
	return nil
}

func (p *Engine) Stop() error {
	p.lock.Lock()
	defer p.lock.Unlock()
	if !p.started {
		return nil
	}
	p.queryExecCtxCache.Store(new(sync.Map)) // Clear the internal state
	p.available.Set(false)
	p.started = false
	return nil
}

func (p *Engine) SetAvailable() error {
	// We execute a query which will fan out to all shards and wait until they are available
	_, err := p.ExecuteQuery("sys", "select * from dummy")
	if err != nil {
		return err
	}
	p.available.Set(true)
	return nil
}

func (p *Engine) BuildPullQuery(execCtx *execctx.ExecutionContext, query string, argTypes []common.ColumnType,
	args []interface{}) (exec.PullExecutor, error) {
	qi := execCtx.QueryInfo
	qi.ExecutionID = execCtx.ID
	qi.SchemaName = execCtx.Schema.Name
	qi.Query = query
	isPs := len(argTypes) != 0
	if isPs {
		// It's a prepared statement
		execCtx.Planner().SetPSArgs(args)
		qi.PsArgTypes = argTypes
		qi.PsArgs = args
		qi.PreparedStatement = true
	}
	ast, paramCount, err := execCtx.Planner().Parse(query)
	if err != nil {
		log.Errorf("failed to parse query %v", err)
		return nil, errors.MaybeConvertToPranaErrorf(err, errors.InvalidStatement, "Invalid statement %s - %s", query, err.Error())
	}
	if paramCount != len(argTypes) {
		return nil, errors.NewPranaErrorf(errors.InvalidParamCount, "Statement has %d param markers but %d param(s) supplied",
			paramCount, len(argTypes))
	}
	logicalPlan, err := execCtx.Planner().BuildLogicalPlan(ast, isPs)
	if err != nil {
		return nil, errors.MaybeConvertToPranaErrorf(err, errors.InvalidStatement, "Invalid statement %s - %s", query, err.Error())
	}
	physicalPlan, err := execCtx.Planner().BuildPhysicalPlan(logicalPlan, true)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return p.buildPullDAGWithOutputNames(execCtx, logicalPlan, physicalPlan, false)
}

// ExecuteRemotePullQuery - executes a pull query received from another node
//nolint:gocyclo
func (p *Engine) ExecuteRemotePullQuery(queryInfo *cluster.QueryExecutionInfo) (*common.Rows, error) {
	// We need to prevent queries being executed before the schemas have been loaded, however queries from the
	// system schema don't need schema to be loaded as that schema is hardcoded in the meta controller
	// In order to actually load other schemas we need to execute queries from the system query so we need a way
	// of executing system queries during the startup process
	if !queryInfo.SystemQuery && !p.available.Get() {
		return nil, errors.NewPranaErrorf(errors.Unavailable, "pull engine not initialised")
	}
	if queryInfo.ExecutionID == "" {
		panic("empty execution id")
	}
	execCtx, ok := p.getCachedExecCtx(queryInfo.ExecutionID)
	newExecution := false
	if !ok {
		schema := p.metaController.GetOrCreateSchema(queryInfo.SchemaName)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		execCtx = execctx.NewExecutionContext(ctx, queryInfo.ExecutionID, schema)
		newExecution = true
		execCtx.QueryInfo = queryInfo
		ast, _, err := execCtx.Planner().Parse(queryInfo.Query)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		if queryInfo.PreparedStatement {
			execCtx.Planner().SetPSArgs(queryInfo.PsArgs)
		}
		logic, err := execCtx.Planner().BuildLogicalPlan(ast, queryInfo.PreparedStatement)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		physicalPlan, err := execCtx.Planner().BuildPhysicalPlan(logic, true)
		if err != nil {
			return nil, err
		}
		dag, err := p.buildPullDAG(execCtx, physicalPlan, true)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		// We only execute the scan on the remote nodes, the rest is done on the originating node
		scan := p.findTableOrIndexScan(dag)
		if scan == nil {
			return nil, errors.Error("cannot find scan")
		}
		execCtx.CurrentQuery = scan
	} else if execCtx.QueryInfo.Query != queryInfo.Query {
		// Sanity check
		panic(fmt.Sprintf("Already executing query is %s but passed in query is %s", execCtx.QueryInfo.Query, queryInfo.Query))
	}
	if queryInfo.Limit == 0 {
		// Zero signals that the query should be closed
		p.execCtxCache().Delete(queryInfo.ExecutionID)
		return CurrentQuery(execCtx).RowsFactory().NewRows(0), nil
	}
	rows, err := p.getRowsFromCurrentQuery(execCtx, int(queryInfo.Limit))
	if err != nil {
		// Make sure we remove current query in case of error
		execCtx.CurrentQuery = nil
		return nil, errors.WithStack(err)
	}
	if newExecution && execCtx.CurrentQuery != nil {
		// We only need to store the ctx for later if there are more rows to return
		p.execCtxCache().Store(queryInfo.ExecutionID, execCtx)
	} else if execCtx.CurrentQuery == nil {
		// We can delete the exec ctx if current query is complete
		p.execCtxCache().Delete(queryInfo.ExecutionID)
	}
	return rows, errors.WithStack(err)
}

func (p *Engine) getRowsFromCurrentQuery(execCtx *execctx.ExecutionContext, limit int) (*common.Rows, error) {
	rows, err := CurrentQuery(execCtx).GetRows(limit)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if rows.RowCount() < limit {
		// Query is complete - we can remove it
		execCtx.CurrentQuery = nil
	}
	return rows, nil
}

func (p *Engine) getCachedExecCtx(ctxID string) (*execctx.ExecutionContext, bool) {
	d, ok := p.execCtxCache().Load(ctxID)
	if !ok {
		return nil, false
	}
	s, ok := d.(*execctx.ExecutionContext)
	if !ok {
		panic("invalid type in remote queries")
	}
	return s, true
}

func (p *Engine) findTableOrIndexScan(executor exec.PullExecutor) exec.PullExecutor {
	ts, ok := executor.(*exec.PullTableScan)
	if ok {
		return ts
	}
	is, ok := executor.(*exec.PullIndexReader)
	if ok {
		return is
	}
	for _, child := range executor.GetChildren() {
		remExecutor := p.findTableOrIndexScan(child)
		if remExecutor != nil {
			return remExecutor
		}
	}
	return nil
}

func (p *Engine) NumCachedExecCtxs() (int, error) {
	numEntries := 0
	p.execCtxCache().Range(func(key, value interface{}) bool {
		numEntries++
		return false
	})
	return numEntries, nil
}

func CurrentQuery(ctx *execctx.ExecutionContext) exec.PullExecutor {
	v := ctx.CurrentQuery
	if v == nil {
		return nil
	}
	cq, ok := v.(exec.PullExecutor)
	if !ok {
		panic("invalid current query type")
	}
	return cq
}

func (p *Engine) NodeJoined(nodeID int) {
}

func (p *Engine) NodeLeft(nodeID int) {
	p.clearExecCtxsForNode(nodeID)
}

func (p *Engine) clearExecCtxsForNode(nodeID int) {
	// The node may have crashed - we remove any exec ctxs for that node
	p.lock.Lock()
	defer p.lock.Unlock()

	var idsToRemove []string
	sNodeID := fmt.Sprintf("%d", nodeID)
	p.execCtxCache().Range(func(key, value interface{}) bool {
		ctxID := key.(string) //nolint: forcetypeassert
		i := strings.Index(ctxID, "-")
		if i == -1 {
			panic(fmt.Sprintf("invalid ctx id %s", ctxID))
		}
		snid := ctxID[:i]
		if snid == sNodeID {
			idsToRemove = append(idsToRemove, ctxID)
		}
		return true
	})
	for _, ctxID := range idsToRemove {
		p.execCtxCache().Delete(ctxID)
	}
}

// ExecuteQuery - Lightweight query interface - used internally for loading a moderate amount of rows
func (p *Engine) ExecuteQuery(schemaName string, query string) (rows *common.Rows, err error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	schema, ok := p.metaController.GetSchema(schemaName)
	if !ok {
		return nil, errors.Errorf("no such schema %s", schemaName)
	}
	execCtx := execctx.NewExecutionContext(ctx, "", schema)
	executor, err := p.BuildPullQuery(execCtx, query, nil, nil)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	limit := 1000
	for {
		r, err := executor.GetRows(limit)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		if rows == nil {
			rows = r
		} else {
			rows.AppendAll(r)
		}
		if r.RowCount() < limit {
			break
		}
	}
	// No need to close execCtx as no prepared statements
	return rows, nil
}

func (p *Engine) execCtxCache() *sync.Map {
	return p.queryExecCtxCache.Load().(*sync.Map)
}
