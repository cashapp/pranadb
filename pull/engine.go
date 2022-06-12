package pull

import (
	"fmt"
	"github.com/squareup/pranadb/sharder"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/command/parser"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/meta"
	"github.com/squareup/pranadb/protos/squareup/cash/pranadb/v1/notifications"
	"github.com/squareup/pranadb/pull/exec"
	"github.com/squareup/pranadb/remoting"
	"github.com/squareup/pranadb/sess"
)

type Engine struct {
	lock               sync.RWMutex
	started            bool
	remoteSessionCache atomic.Value
	cluster            cluster.Cluster
	metaController     *meta.Controller
	nodeID             int
	shrder             *sharder.Sharder
	available          common.AtomicBool
}

func NewPullEngine(cluster cluster.Cluster, metaController *meta.Controller, shrder *sharder.Sharder) *Engine {
	engine := Engine{
		cluster:        cluster,
		metaController: metaController,
		nodeID:         cluster.GetNodeID(),
		shrder:         shrder,
	}
	engine.remoteSessionCache.Store(new(sync.Map))
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
	p.remoteSessionCache.Store(new(sync.Map)) // Clear the internal state
	p.available.Set(false)
	p.started = false
	return nil
}

func (p *Engine) SetAvailable() {
	p.available.Set(true)
}

// PrepareSQLStatement prepares a SQL statement
// When a statement is prepared we parse it to get the AST and create a PreparedStatement struct which we cache
// in the session. When we execute it the first time we build the logical plan and cache it - we can't build the
// logical plan in the prepare stage as we need to know the types of the arguments to build the plan.
// We don't cache the physical plan - we build it every time as it can depend on the args - e.g. for a point get
// we push the sel to the table scan as a unitary range - this would be different depending on the args.
func (p *Engine) PrepareSQLStatement(session *sess.Session, sql string) (exec.PullExecutor, error) {
	ast, err := parser.Parse(sql)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if ast.Select == "" {
		return nil, errors.Errorf("only sql queries can be prepared %s", sql)
	}
	tiAst, err := session.Planner().Parse(sql)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	psID := session.GeneratePSId()
	ps := session.CreatePreparedStatement(psID, sql, tiAst)
	session.PsCache[psID] = ps
	return exec.NewSingleValueBigIntRow(psID, "PS_ID"), nil
}

func (p *Engine) ExecutePreparedStatement(session *sess.Session, psID int64, args []interface{}) (exec.PullExecutor, error) {
	ps, ok := session.PsCache[psID]
	if !ok {
		return nil, errors.NewUnknownPreparedStatementError(psID)
	}
	// Ps args on the planner are what are used when retrieving ps args when evaluating expressions on the dag
	session.Planner().SetPSArgs(args)
	// We also need to set them on the queryinfo - this is what gets passed remotely to the target node
	session.QueryInfo.PsArgs = args

	if ps.LogicalPlan == nil {
		qi := session.QueryInfo
		qi.PsArgs = args
		// TODO pass properlym typed arguments via gRPC API
		qi.PsArgTypes = make([]common.ColumnType, len(args))
		for i := 0; i < len(args); i++ {
			qi.PsArgTypes[i] = common.InferColumnType(args[i])
		}
		qi.SessionID = session.ID
		qi.SchemaName = session.Schema.Name
		qi.Query = ps.Query
		qi.PsID = ps.ID
		qi.IsPs = true
		logic, err := session.Planner().BuildLogicalPlan(ps.Ast, true)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		ps.LogicalPlan = logic
	}
	physicalPlan, err := session.Planner().BuildPhysicalPlan(ps.LogicalPlan, true)
	if err != nil {
		return nil, err
	}
	return p.buildPullDAGWithOutputNames(session, ps.LogicalPlan, physicalPlan, false)
}

func (p *Engine) BuildPullQuery(session *sess.Session, query string) (exec.PullExecutor, error) {
	qi := session.QueryInfo
	qi.SessionID = session.ID
	qi.SchemaName = session.Schema.Name
	qi.Query = query
	qi.IsPs = false
	ast, err := session.Planner().Parse(query)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	logicalPlan, err := session.Planner().BuildLogicalPlan(ast, false)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	physicalPlan, err := session.Planner().BuildPhysicalPlan(logicalPlan, true)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return p.buildPullDAGWithOutputNames(session, logicalPlan, physicalPlan, false)
}

// ExecuteRemotePullQuery - executes a pull query received from another node
//nolint:gocyclo
func (p *Engine) ExecuteRemotePullQuery(queryInfo *cluster.QueryExecutionInfo) (*common.Rows, error) {

	// We need to prevent queries being executed before the schemas have been loaded, however queries from the
	// system schema don't need schema to be loaded as that schema is hardcoded in the meta controller
	// In order to actually load other schemas we need to execute queries from the system query so we need a way
	// of executing system queries during the startup process
	if !queryInfo.SystemQuery && !p.available.Get() {
		return nil, errors.New("pull engine not available")
	}

	if queryInfo.SessionID == "" {
		panic("empty session id")
	}

	schema := p.metaController.GetOrCreateSchema(queryInfo.SchemaName)
	s, ok := p.getCachedSession(queryInfo.SessionID)
	newSession := false
	if !ok {
		s = sess.NewSession(queryInfo.SessionID, nil)
		newSession = true
	}

	// We lock the session, not because of concurrent access but because we need a memory barrier
	// as the session is mutated on subsequent calls which can be on different goroutines
	s.Lock.Lock()
	defer s.Lock.Unlock()
	s.UseSchema(schema)
	if s.CurrentQuery == nil {
		s.QueryInfo = queryInfo
		s.Planner().SetPSArgs(queryInfo.PsArgs)

		if queryInfo.IsPs {
			// Prepared Statement
			ps, ok := s.PsCache[queryInfo.PsID]
			if !ok {
				ast, err := s.Planner().Parse(queryInfo.Query)
				if err != nil {
					return nil, errors.WithStack(err)
				}
				logic, err := s.Planner().BuildLogicalPlan(ast, true)
				if err != nil {
					return nil, errors.WithStack(err)
				}
				ps = s.CreateRemotePreparedStatement(queryInfo.PsID, queryInfo.Query)
				ps.LogicalPlan = logic
				s.PsCache[queryInfo.PsID] = ps
			}
			// We build the physical plan each time
			physicalPlan, err := s.Planner().BuildPhysicalPlan(ps.LogicalPlan, true)
			if err != nil {
				return nil, err
			}
			dag, err := p.buildPullDAG(s, physicalPlan, false)
			if err != nil {
				return nil, errors.WithStack(err)
			}
			remExecutor := p.findRemoteExecutor(dag)
			if remExecutor == nil {
				return nil, errors.Error("cannot find remote executor")
			}
			s.CurrentQuery = remExecutor.RemoteDag
		} else {
			ast, err := s.Planner().Parse(queryInfo.Query)
			if err != nil {
				return nil, errors.WithStack(err)
			}
			logic, err := s.Planner().BuildLogicalPlan(ast, true)
			if err != nil {
				return nil, errors.WithStack(err)
			}
			physicalPlan, err := s.Planner().BuildPhysicalPlan(logic, true)
			if err != nil {
				return nil, err
			}
			dag, err := p.buildPullDAG(s, physicalPlan, false)
			if err != nil {
				return nil, errors.WithStack(err)
			}
			remExecutor := p.findRemoteExecutor(dag)
			if remExecutor == nil {
				return nil, errors.Error("cannot find remote executor")
			}
			s.CurrentQuery = remExecutor.RemoteDag
		}
	} else if s.QueryInfo.Query != queryInfo.Query {
		// Sanity check
		panic(fmt.Sprintf("Already executing query is %s but passed in query is %s", s.QueryInfo.Query, queryInfo.Query))
	}
	rows, err := p.getRowsFromCurrentQuery(s, int(queryInfo.Limit))
	if err != nil {
		// Make sure we remove current query in case of error
		s.CurrentQuery = nil
		return nil, errors.WithStack(err)
	}
	if newSession {
		// We only need to store the session for later if there is an outstanding query or there are prepared statements
		if len(s.PsCache) != 0 || s.CurrentQuery != nil {
			p.sessionCache().Store(queryInfo.SessionID, s)
		}
	} else {
		// We can delete the session if there are no more prepared statements or if current query is complete
		if len(s.PsCache) == 0 && s.CurrentQuery == nil {
			p.sessionCache().Delete(queryInfo.SessionID)
		}
	}
	return rows, errors.WithStack(err)
}

func (p *Engine) getRowsFromCurrentQuery(session *sess.Session, limit int) (*common.Rows, error) {
	rows, err := CurrentQuery(session).GetRows(limit)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if rows.RowCount() < limit {
		// Query is complete - we can remove it
		session.CurrentQuery = nil
	}
	return rows, nil
}

func (p *Engine) getCachedSession(sessionID string) (*sess.Session, bool) {
	d, ok := p.sessionCache().Load(sessionID)
	if !ok {
		return nil, false
	}
	s, ok := d.(*sess.Session)
	if !ok {
		panic("invalid type in remote queries")
	}
	return s, true
}

func (p *Engine) findRemoteExecutor(executor exec.PullExecutor) *exec.RemoteExecutor {
	// We only execute the part of the dag beyond the table reader - this is the remote part
	remExecutor, ok := executor.(*exec.RemoteExecutor)
	if ok {
		return remExecutor
	}
	for _, child := range executor.GetChildren() {
		remExecutor := p.findRemoteExecutor(child)
		if remExecutor != nil {
			return remExecutor
		}
	}
	return nil
}

func (p *Engine) NumCachedSessions() (int, error) {
	numEntries := 0
	p.sessionCache().Range(func(key, value interface{}) bool {
		numEntries++
		return false
	})
	return numEntries, nil
}

func (p *Engine) HandleMessage(notification remoting.ClusterMessage) (remoting.ClusterMessage, error) {
	sessCloseMsg := notification.(*notifications.SessionClosedMessage) // nolint: forcetypeassert
	p.sessionCache().Delete(sessCloseMsg.GetSessionId())
	return nil, nil
}

func CurrentQuery(session *sess.Session) exec.PullExecutor {
	v := session.CurrentQuery
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
	p.clearSessionsForNode(nodeID)
}

func (p *Engine) clearSessionsForNode(nodeID int) {
	// The node may have crashed - we remove any sessions for that node
	p.lock.Lock()
	defer p.lock.Unlock()

	var idsToRemove []string
	sNodeID := fmt.Sprintf("%d", nodeID)
	p.sessionCache().Range(func(key, value interface{}) bool {
		sessionID := key.(string) //nolint: forcetypeassert
		i := strings.Index(sessionID, "-")
		if i == -1 {
			panic(fmt.Sprintf("invalid session id %s", sessionID))
		}
		snid := sessionID[:i]
		if snid == sNodeID {
			idsToRemove = append(idsToRemove, sessionID)
		}
		return true
	})
	for _, sessID := range idsToRemove {
		p.sessionCache().Delete(sessID)
	}
}

// ExecuteQuery - Lightweight query interface - used internally for loading a moderate amount of rows
func (p *Engine) ExecuteQuery(schemaName string, query string) (rows *common.Rows, err error) {
	schema, ok := p.metaController.GetSchema(schemaName)
	if !ok {
		return nil, errors.Errorf("no such schema %s", schemaName)
	}
	sess := sess.NewSession("", nil)
	sess.UseSchema(schema)
	exec, err := p.BuildPullQuery(sess, query)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	limit := 1000
	for {
		r, err := exec.GetRows(limit)
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
	// No need to close session as no prepared statements
	return rows, nil
}

func (p *Engine) sessionCache() *sync.Map {
	return p.remoteSessionCache.Load().(*sync.Map)
}
