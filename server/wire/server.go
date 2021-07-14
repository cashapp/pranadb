// Package wire contains the over-the-wire gRPC server for PranaDB.
package wire

import (
	"context"

	pranaproto "github.com/squareup/pranadb/protos/squareup/cash/pranadb/service"
	"github.com/squareup/pranadb/server"
)

// Server over gRPC.
type Server struct {
	server *server.Server
}

func (s *Server) GetMaterializedView(context.Context, *pranaproto.GetMaterializedViewRequest) (*pranaproto.GetMaterializedViewResponse, error) {
	return nil, nil
}

func New(server *server.Server) *Server {
	return &Server{server}
}
