package sessctx

import (
	"fmt"
	"github.com/squareup/pranadb/tidb/infoschema"
	"github.com/squareup/pranadb/tidb/sessionctx"
	"github.com/squareup/pranadb/tidb/sessionctx/variable"
	"github.com/squareup/pranadb/tidb/types"
)

func NewSessionContext(is infoschema.InfoSchema, pullQuery bool, database string) *SessCtx {
	sessVars := variable.NewSessionVars()
	// This is necessary to ensure prepared statement param markers are created properly in the
	// plan
	sessVars.StmtCtx.UseCache = true
	sessVars.CurrentDB = database

	ctx := SessCtx{
		is:          is,
		values:      make(map[fmt.Stringer]interface{}),
		sessionVars: sessVars,
	}
	return &ctx
}

func toDatums(args []interface{}) variable.PreparedParams {
	pp := make([]types.Datum, len(args))
	for i, arg := range args {
		pp[i] = types.Datum{}
		pp[i].SetValueWithDefaultCollation(arg)
	}
	return pp
}

func NewDummySessionContext() sessionctx.Context {
	return NewSessionContext(nil, false, "test")
}

type SessCtx struct {
	is          infoschema.InfoSchema
	sessionVars *variable.SessionVars
	values      map[fmt.Stringer]interface{}
}

func (s *SessCtx) SetInfoSchema(infoSchema infoschema.InfoSchema) {
	s.is = infoSchema
}

func (s *SessCtx) SetArgs(args []interface{}) {
	s.sessionVars.PreparedParams = toDatums(args)
}

func (s *SessCtx) SetValue(key fmt.Stringer, value interface{}) {
	s.values[key] = value
}

func (s SessCtx) Value(key fmt.Stringer) interface{} {
	value := s.values[key]
	return value
}

func (s SessCtx) GetInfoSchema() sessionctx.InfoschemaMetaVersion {
	return s.is
}

func (s SessCtx) GetSessionVars() *variable.SessionVars {
	return s.sessionVars
}
