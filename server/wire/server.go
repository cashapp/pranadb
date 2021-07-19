// Package wire contains the over-the-wire gRPC server for PranaDB.
package wire

import (
	pranaproto "github.com/squareup/pranadb/protos/squareup/cash/pranadb/v1/service"
	"github.com/squareup/pranadb/server"
)

// Server over gRPC.
type Server struct {
	server *server.Server
}

func (s Server) ExecuteSQLStatement(request *pranaproto.ExecuteSQLStatementRequest, statementServer pranaproto.PranaDBService_ExecuteSQLStatementServer) error {
	panic("implement me")
}

var _ pranaproto.PranaDBServiceServer = &Server{}

func New(server *server.Server) *Server {
	return &Server{server}
}
