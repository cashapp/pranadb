// Package wire contains the over-the-wire gRPC server for PranaDB.
package wire

import (
	"io"
	"log"

	"github.com/pkg/errors"

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
}

func New(server *server.Server) *Server {
	return &Server{server}
}

var _ service.PranaDBServiceServer = &Server{}

func (s *Server) ExecuteSQLStatement(stream service.PranaDBService_ExecuteSQLStatementServer) error {
	log.Printf("New SQL session started")
	var session *sess.Session
	for {
		in, err := stream.Recv()
		if err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}
		if in.PageSize == 0 {
			if _, err = sendError(stream, errors.New("invalid page_size 0")); err != nil {
				return err
			}
			continue
		}
		newSession, executor, err := s.processQuery(session, in.Query)
		if ok, serr := sendError(stream, err); serr != nil {
			return serr
		} else if !ok {
			continue
		}

		if newSession != nil {
			session = newSession
			log.Printf("Switched to schema: %s", session.Schema.Name)
		}

		if executor != nil {
			if err = s.streamPages(stream, int(in.PageSize), executor); err != nil {
				return err
			}
		}
	}
}

func (s *Server) streamPages(stream service.PranaDBService_ExecuteSQLStatementServer, requestedPageSize int, executor exec.PullExecutor) error {
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
			More:  pageSize == requestedPageSize,
		}
		if err = stream.Send(&service.ExecuteSQLStatementResponse{Result: &service.ExecuteSQLStatementResponse_Page{Page: results}}); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func (s *Server) processQuery(session *sess.Session, query string) (*sess.Session, exec.PullExecutor, error) {
	log.Printf("exec: %s", query)
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
			return nil, nil, errors.Errorf("no schema selected - USE <schema>")
		}
		exec, err := s.server.GetCommandExecutor().ExecuteSQLStatement(session, query)
		if err != nil {
			return nil, nil, errors.WithStack(err)
		}
		return nil, exec, nil
	}
}

// Returns ok=true if we should continue processing, serr!=nil if sending failed and we should abort the stream.
func sendError(stream service.PranaDBService_ExecuteSQLStatementServer, err error) (ok bool, serr error) {
	if err == nil {
		return true, nil
	}
	return false, errors.WithStack(stream.Send(&service.ExecuteSQLStatementResponse{
		Result: &service.ExecuteSQLStatementResponse_Error{Error: err.Error()},
	}))
}
