package command

import (
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/parplan"
	"github.com/squareup/pranadb/pull/exec"
)

// Session represents a user's session with Prana
// There will be typically be one session for the duration of a client's connection with Prana.
// The session contains the parser/planner (which is not thread-safe) and the current schema name
// The default schema on connect is determined at user login from their user account information.
// It can be changed by a USE <schema_name> command, if the user is an admin.
type Session struct {
	ce     *Executor
	schema *common.Schema
	pl     *parplan.Planner
}

func NewSession(ce *Executor, schema *common.Schema) *Session {
	return &Session{
		ce:     ce,
		schema: schema,
		pl:     parplan.NewPlanner(),
	}
}

// ExecuteSQLStatement executes a synchronous SQL statement.
func (s *Session) ExecuteSQLStatement(sql string) (exec.PullExecutor, error) {
	return s.ce.executeSQLStatementInternal(s.pl, s.schema, sql, true, nil)
}
