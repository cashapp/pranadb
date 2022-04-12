package sess

import (
	"github.com/squareup/pranadb/tidb/planner"
	"sync"
	"sync/atomic"

	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/parplan"
)

// Session represents a user's session with Prana
// There will be typically be one session for the duration of a client's connection with Prana.
// The session contains the parser/planner (which is not thread-safe) and the current schema name
// The default schema on connect is determined at user login from their user account information.
// It can be changed by a USE <schema_name> command, if the user is an admin.
type Session struct {
	ID           string
	Schema       *common.Schema
	planner      *parplan.Planner
	PsCache      map[int64]*PreparedStatement
	stmtSequence int64
	QueryInfo    *cluster.QueryExecutionInfo
	CurrentQuery interface{} // TODO find a better way - typed as interface{} to avoid circular dependency with pull
	Lock         sync.Mutex
	sessCloser   RemoteSessionCloser
}

func NewSession(id string, sessCloser RemoteSessionCloser) *Session {
	return &Session{
		ID:           id,
		PsCache:      make(map[int64]*PreparedStatement),
		QueryInfo:    new(cluster.QueryExecutionInfo),
		stmtSequence: -1,
		sessCloser:   sessCloser,
	}
}

func (s *Session) UseSchema(schema *common.Schema) {
	if s.Schema == nil || s.Schema != schema {
		s.Schema = schema
		s.planner = nil
	}
}

func (s *Session) Planner() *parplan.Planner {
	if s.planner == nil {
		s.planner = parplan.NewPlanner(s.Schema)
	}
	return s.planner
}

func (s *Session) GeneratePSId() int64 {
	// Although sessions aren't accessed concurrently - they can be accessed at different times by different goroutines
	// so we still need a memory barrier
	return atomic.AddInt64(&s.stmtSequence, 1)
}

func (s *Session) CreatePreparedStatement(id int64, query string, ast parplan.AstHandle) *PreparedStatement {
	return &PreparedStatement{
		ID:    id,
		Query: query,
		Ast:   ast,
	}
}

func (s *Session) CreateRemotePreparedStatement(id int64, query string) *PreparedStatement {
	return &PreparedStatement{
		ID:    id,
		Query: query,
	}
}

type PreparedStatement struct {
	ID          int64
	Query       string
	LogicalPlan planner.LogicalPlan
	Ast         parplan.AstHandle
}

// Abort should be invoked if the session might have running queries
func (s *Session) Abort() error {
	return s.sessCloser.CloseRemoteSessions(s.ID)
}

// Close must only be invoked if no queries are running
func (s *Session) Close() error {
	if len(s.PsCache) != 0 {
		// We will only have remote sessions if we have prepared statements so we don't need to broadcast session
		// close otherwise
		return s.sessCloser.CloseRemoteSessions(s.ID)
	}
	return nil
}

type RemoteSessionCloser interface {
	CloseRemoteSessions(sessionID string) error
}
