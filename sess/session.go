package sess

import (
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/parplan"
)

// Session represents a user's session with Prana
// There will be typically be one session for the duration of a client's connection with Prana.
// The session contains the parser/planner (which is not thread-safe) and the current schema name
// The default schema on connect is determined at user login from their user account information.
// It can be changed by a USE <schema_name> command, if the user is an admin.
type Session struct {
	Schema *common.Schema
	Pl     *parplan.Planner
}

func NewSession(schema *common.Schema, pl *parplan.Planner) *Session {
	return &Session{
		Schema: schema,
		Pl:     pl,
	}
}
