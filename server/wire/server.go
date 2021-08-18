// Package wire contains the over-the-wire gRPC server for PranaDB.
package wire

import (
	"context"

	"github.com/pkg/errors"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/squareup/pranadb/command/parser"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/protos/squareup/cash/pranadb/v1/service"
	"github.com/squareup/pranadb/pull/exec"
	"github.com/squareup/pranadb/server"
	"github.com/squareup/pranadb/sess"
)

// Server over gRPC.
type Server struct {
	server *server.Server
	logger *zap.Logger
}

func New(server *server.Server, logger *zap.Logger) *Server {
	return &Server{server, logger}
}

var _ service.PranaDBServiceServer = &Server{}

func (s *Server) Use(ctx context.Context, request *service.UseRequest) (*emptypb.Empty, error) {
	session := s.server.GetCommandExecutor().CreateSession(request.Schema)
	SetSession(ctx, s.logger, session)
	return &emptypb.Empty{}, nil
}

func (s *Server) ExecuteSQLStatement(in *service.ExecuteSQLStatementRequest, stream service.PranaDBService_ExecuteSQLStatementServer) error {
	session := SessionFromContext(stream.Context())
	newSession, executor, err := s.processQuery(session, in.Query)
	if err != nil {
		return err
	}

	// Have we switched schemas?
	if newSession != nil {
		session = newSession
		SetSession(stream.Context(), s.logger, session)
		if executor == nil {
			return nil
		}
	}

	if session == nil {
		return status.Errorf(codes.FailedPrecondition, "no schema selected - USE <schema>")
	}

	// First send column definitions.
	columns := &service.Columns{}
	names := executor.ColNames()
	for i, typ := range executor.ColTypes() {
		name := ""
		if i < len(names) {
			name = names[i]
		}
		column := &service.Column{
			Name: name,
			Type: service.ColumnType(typ.Type),
		}
		if typ.Type == common.TypeDecimal {
			column.DecimalParams = &service.DecimalParams{
				DecimalPrecision: uint32(typ.DecPrecision),
				DecimalScale:     uint32(typ.DecScale),
			}
		}
		columns.Columns = append(columns.Columns, column)
	}
	if err := stream.Send(&service.ExecuteSQLStatementResponse{Result: &service.ExecuteSQLStatementResponse_Columns{Columns: columns}}); err != nil {
		return errors.WithStack(err)
	}

	// Then start sending pages until complete.
	requestedPageSize := int(in.PageSize)
	pageSize := requestedPageSize
	for pageSize >= requestedPageSize {
		// Transcode rows.
		rows, err := executor.GetRows(requestedPageSize)
		if err != nil {
			return errors.WithStack(err)
		}
		pageSize = rows.RowCount()
		results := &service.Page{
			Count: uint64(pageSize),
			Rows:  rows.Serialize(),
		}
		if err = stream.Send(&service.ExecuteSQLStatementResponse{Result: &service.ExecuteSQLStatementResponse_Page{Page: results}}); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func (s *Server) processQuery(session *sess.Session, query string) (*sess.Session, exec.PullExecutor, error) {
	ast, err := parser.Parse(query)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	switch {
	case ast.Use != "":
		newSession := s.server.GetCommandExecutor().CreateSession(ast.Use)
		return newSession, exec.NewStaticRow(), nil

	case ast.Create != nil && ast.Create.Schema != "":
		newSession := s.server.GetCommandExecutor().CreateSession(ast.Create.Schema)
		return newSession, exec.NewStaticRow(), nil

	default:
		if session == nil {
			return nil, nil, status.Errorf(codes.FailedPrecondition, "no schema selected - USE <schema>")
		}
		exec, err := s.server.GetCommandExecutor().ExecuteSQLStatement(session, query)
		if err != nil {
			return nil, nil, errors.WithStack(err)
		}
		return nil, exec, nil
	}
}
