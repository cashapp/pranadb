package pull

import (
	"fmt"
	"github.com/cznic/mathutil"
	"github.com/pkg/errors"
	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/command/parser"
	"github.com/squareup/pranadb/common"
	errors2 "github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/meta"
	"github.com/squareup/pranadb/notifier"
	"github.com/squareup/pranadb/protos/squareup/cash/pranadb/v1/notifications"
	"github.com/squareup/pranadb/pull/exec"
	"github.com/squareup/pranadb/sess"
	"strings"
	"sync"
)

type PullEngine struct {
	lock               sync.RWMutex
	started            bool
	remoteSessionCache sync.Map // TODO if a node dies we need to remove all sessions for that node or they can be orphaned
	cluster            cluster.Cluster
	metaController     *meta.Controller
	nodeID             int
}

func NewPullEngine(cluster cluster.Cluster, metaController *meta.Controller) *PullEngine {
	engine := PullEngine{
		cluster:        cluster,
		metaController: metaController,
		nodeID:         cluster.GetNodeID(),
	}
	return &engine
}

func (p *PullEngine) Start() error {
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.started {
		return nil
	}
	p.started = true
	return nil
}

func (p *PullEngine) Stop() {
	p.lock.Lock()
	defer p.lock.Unlock()
	if !p.started {
		return
	}
	p.started = false
}

// PrepareSQLStatement prepares a SQL statement
// When a statement is prepared we parse it to get the AST and create a PreparedStatement struct which we cache
// in the session.
// When the statement is executed the first time, a execution DAG will be built from the AST and the arguments
// We can't build the DAG before seeing real arguments as the planner needs the types.
// We then cache the DAG so the second time it is executed we reuse the same DAG.
func (p *PullEngine) PrepareSQLStatement(session *sess.Session, sql string) (exec.PullExecutor, error) {
	ast, err := parser.Parse(sql)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if ast.Select == "" {
		return nil, fmt.Errorf("only sql queries can be prepared %s", sql)
	}
	tiAst, err := session.PullPlanner().Parse(sql)
	if err != nil {
		return nil, err
	}
	psID := session.GeneratePSId()
	ps := session.CreatePreparedStatement(psID, sql, tiAst)
	session.PsCache[psID] = ps
	return exec.NewStaticRow(psID), nil
}

func (p *PullEngine) ExecutePreparedStatement(session *sess.Session, psID int64, args []interface{}) (exec.PullExecutor, error) {
	ps, ok := session.PsCache[psID]
	if !ok {
		return nil, errors2.NewUserErrorF(errors2.PreparedStatementDoesNotExist, "cannot find prepared statement with id %d", psID)
	}
	// Ps args on the planner are what are used when retrieving ps args when evaluating expressions on the dag
	session.PullPlanner().SetPSArgs(args)
	// And we need to refresh the info schema - so the query works against the state of the database metadata as of now
	session.PullPlanner().RefreshInfoSchema()
	// We also need to set them on the queryinfo - this is what gets passed remotely to the target node
	session.QueryInfo.PsArgs = args
	if ps.Dag == nil {
		qi := session.QueryInfo
		qi.PsArgs = args
		// TODO unfortunately args are always passed as strings so this always results in type varchar
		// We should find someone way of finding out the expected col types of the args so
		// we can convert them appropriately.
		// It works with varchar as TiDB will cast them at runtime but that's probably less efficient
		qi.PsArgTypes = make([]common.ColumnType, len(args))
		for i := 0; i < len(args); i++ {
			qi.PsArgTypes[i] = common.InferColumnType(args[i])
		}
		qi.SessionID = session.ID
		qi.SchemaName = session.Schema.Name
		qi.Query = ps.Query
		qi.PsID = ps.ID
		qi.IsPs = true
		dag, err := p.buildPullQueryExecutionFromAst(session, ps.Ast, true, false)
		if err != nil {
			return nil, err
		}
		ps.Dag = dag
	} else {
		// We're reusing the dag so we need to reset it - PullExecutors are stateful like cursors
		ps.Dag.Reset()
	}
	return ps.Dag, nil
}

func (p *PullEngine) BuildPullQuery(session *sess.Session, query string) (exec.PullExecutor, error) {
	qi := session.QueryInfo
	qi.SessionID = session.ID
	qi.SchemaName = session.Schema.Name
	qi.Query = query
	qi.IsPs = false
	session.PullPlanner().RefreshInfoSchema()
	return p.buildPullQueryExecutionFromQuery(session, query, false, false)
}

func (p *PullEngine) ExecuteRemotePullQuery(queryInfo *cluster.QueryExecutionInfo) (*common.Rows, error) {
	if queryInfo.SessionID == "" {
		panic("empty session id")
	}

	schema := p.metaController.GetOrCreateSchema(queryInfo.SchemaName)
	s, ok := p.getCachedSession(queryInfo.SessionID)
	newSession := false
	if !ok {
		s = sess.NewSession(queryInfo.SessionID, schema, nil)
		newSession = true
	}
	// We lock the session, not because of concurrent access but because we need a memory barrier
	// as the session is mutated on subsequent calls which can be on different goroutines
	s.Lock.Lock()
	defer s.Lock.Unlock()
	if s.CurrentQuery == nil {
		s.QueryInfo = queryInfo
		s.PullPlanner().SetPSArgs(queryInfo.PsArgs)
		s.PullPlanner().RefreshInfoSchema()
		if queryInfo.IsPs {
			// Prepared Statement
			ps, ok := s.PsCache[queryInfo.PsID]
			if !ok {
				// Not already prepared
				dag, err := p.buildPullQueryExecutionFromQuery(s, queryInfo.Query, true, true)
				if err != nil {
					return nil, err
				}
				remExecutor := p.findRemoteExecutor(dag)
				if remExecutor == nil {
					return nil, errors.New("cannot find remote executor")
				}
				s.CurrentQuery = remExecutor.RemoteDag
				ps = s.CreateRemotePreparedStatement(queryInfo.PsID, queryInfo.Query)
				ps.Dag = CurrentQuery(s)
				s.PsCache[queryInfo.PsID] = ps
			} else {
				// Already prepared
				s.CurrentQuery = ps.Dag
				// We're reusing it so it needs to be reset
				CurrentQuery(s).Reset()
			}
		} else {
			dag, err := p.buildPullQueryExecutionFromQuery(s, queryInfo.Query, false, true)
			if err != nil {
				return nil, err
			}
			remExecutor := p.findRemoteExecutor(dag)
			if remExecutor == nil {
				return nil, errors.New("cannot find remote executor")
			}
			s.CurrentQuery = remExecutor.RemoteDag
		}
	}
	rows, err := p.getRowsFromCurrentQuery(s, int(queryInfo.Limit))
	if newSession {
		// We only need to store the session for later if there is an outstanding query or there are prepared statements
		if len(s.PsCache) != 0 || s.CurrentQuery != nil {
			p.remoteSessionCache.Store(queryInfo.SessionID, s)
		}
	}
	return rows, err
}

func (p *PullEngine) getRowsFromCurrentQuery(session *sess.Session, limit int) (*common.Rows, error) {
	rows, err := CurrentQuery(session).GetRows(limit)
	if err != nil {
		return nil, err
	}
	if rows.RowCount() < limit {
		// Query is complete - we can remove it
		session.CurrentQuery = nil
	}
	return rows, nil
}

func (p *PullEngine) getCachedSession(sessionID string) (*sess.Session, bool) {
	d, ok := p.remoteSessionCache.Load(sessionID)
	if !ok {
		return nil, false
	}
	s, ok := d.(*sess.Session)
	if !ok {
		panic("invalid type in remote queries")
	}
	return s, true
}

func (p *PullEngine) findRemoteExecutor(executor exec.PullExecutor) *exec.RemoteExecutor {
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

func (p *PullEngine) NumCachedSessions() (int, error) {
	numEntries := 0
	p.remoteSessionCache.Range(func(key, value interface{}) bool {
		numEntries++
		return false
	})
	return numEntries, nil
}

func (p *PullEngine) HandleNotification(notification notifier.Notification) {
	sessCloseMsg := notification.(*notifications.SessionClosedMessage) // nolint: forcetypeassert
	p.remoteSessionCache.Delete(sessCloseMsg.GetSessionId())
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

func (p *PullEngine) NodeJoined(nodeID int) {
}

func (p *PullEngine) NodeLeft(nodeID int) {
	p.clearSessionsForNode(nodeID)
}

func (p *PullEngine) clearSessionsForNode(nodeID int) {
	// The node may have crashed - we remove any sessions for that node
	p.lock.Lock()
	defer p.lock.Unlock()

	var idsToRemove []string
	sNodeID := fmt.Sprintf("%d", nodeID)
	p.remoteSessionCache.Range(func(key, value interface{}) bool {
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
		p.remoteSessionCache.Delete(sessID)
	}
}

// ExecuteQuery - Lightweight query interface - used internally for loading a moderate amount of rows
func (p *PullEngine) ExecuteQuery(schemaName string, query string) (rows *common.Rows, err error) {
	schema, ok := p.metaController.GetSchema(schemaName)
	if !ok {
		return nil, fmt.Errorf("no such schema %s", schemaName)
	}
	sess := sess.NewSession("", schema, nil)
	exec, err := p.BuildPullQuery(sess, query)
	if err != nil {
		return nil, err
	}
	// The query is one shot, so remote session will not be stored, so session does not need to be closed
	// and have close broadcast
	rows, err = exec.GetRows(mathutil.MaxInt)
	if err != nil {
		return nil, err
	}
	return rows, nil
}
