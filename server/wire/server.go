// Package wire contains the over-the-wire gRPC server for PranaDB.
package wire

import (
	"io"
	"log"

	"github.com/pkg/errors"

	"github.com/squareup/pranadb/command/parser"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/parplan"
	"github.com/squareup/pranadb/protos/squareup/cash/pranadb/v1/service"
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
		newSession, results, err := s.processQuery(session, in.Query)
		if ok, serr := sendError(stream, err); serr != nil {
			return serr
		} else if !ok {
			continue
		} else {
			if newSession != nil {
				session = newSession
				log.Printf("Switched to schema: %s", session.Schema.Name)
			}
			err = stream.Send(&service.ExecuteSQLStatementResponse{
				Result: &service.ExecuteSQLStatementResponse_Results{Results: results},
			})
			if err != nil {
				return errors.WithStack(err)
			}
		}
	}
}

func (s *Server) processQuery(session *sess.Session, query string) (*sess.Session, *service.ResultSet, error) {
	log.Printf("exec: %s", query)
	ast, err := parser.Parse(query)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	switch {
	case ast.Use != "":
		schema, ok := s.server.GetMetaController().GetSchema(ast.Use)
		if !ok {
			return nil, nil, errors.Errorf("no such schema %q", ast.Use)
		}
		newSession := sess.NewSession(schema, parplan.NewPlanner())
		return newSession, &service.ResultSet{}, nil

	case ast.Create != nil && ast.Create.Schema != "":
		schema := s.server.GetMetaController().GetOrCreateSchema(ast.Create.Schema)
		newSession := sess.NewSession(schema, parplan.NewPlanner())
		return newSession, &service.ResultSet{}, nil

	default:
		if session == nil {
			return nil, nil, errors.Errorf("no schema selected - USE <schema>")
		}
		exec, err := s.server.GetCommandExecutor().ExecuteSQLStatement(session, query)
		if err != nil {
			return nil, nil, errors.WithStack(err)
		}
		results := &service.ResultSet{}
		// Translate column definitions.
		names := exec.ColNames()
		for i, typ := range exec.ColTypes() {
			name := ""
			if i < len(names) {
				name = names[i]
			}
			column := &service.Column{
				Name: name,
				Type: service.ColumnType(typ.Type),
			}
			if typ.Type == common.TypeDecimal {
				column.DecimalParams = &service.DecimalParams{}
			}
			results.Columns = append(results.Columns, column)
		}
		// Transcode rows.
		rows, err := exec.GetRows(1000)
		if err != nil {
			return nil, nil, errors.WithStack(err)
		}
		results.Rows = rows.Serialize()
		return nil, results, nil
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
