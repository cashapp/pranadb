// Package api contains the over-the-wire gRPC server for PranaDB.
package api

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/squareup/pranadb/command"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"log"
	"net"
	"sync"
	"sync/atomic"

	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/protos/squareup/cash/pranadb/v1/service"
)

// Server over gRPC.
type Server struct {
	lock          sync.Mutex
	started       bool
	ce            *command.Executor
	serverAddress string
	gsrv          *grpc.Server
	errorSequence int64
}

func NewAPIServer(ce *command.Executor, serverAddress string) *Server {
	return &Server{
		ce:            ce,
		serverAddress: serverAddress,
	}
}

func (s *Server) Start() error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.started {
		return nil
	}
	list, err := net.Listen("tcp", s.serverAddress)
	if err != nil {
		return err
	}
	s.gsrv = grpc.NewServer(RegisterSessionManager())
	reflection.Register(s.gsrv)
	service.RegisterPranaDBServiceServer(s.gsrv, s)
	s.started = true
	go s.startServer(list)
	return nil
}

func (s *Server) startServer(list net.Listener) {
	err := s.gsrv.Serve(list) //nolint:ifshort
	s.lock.Lock()
	defer s.lock.Unlock()
	s.started = false
	if err != nil {
		log.Printf("grpc server listen failed: %v", err)
	}
}

func (s *Server) Stop() error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if !s.started {
		return nil
	}
	s.gsrv.Stop()
	return nil
}

var _ service.PranaDBServiceServer = &Server{}

func (s *Server) ExecuteSQLStatement(in *service.ExecuteSQLStatementRequest, stream service.PranaDBService_ExecuteSQLStatementServer) error {
	session := SessionFromContext(stream.Context())
	newSession, executor, err := s.ce.ExecuteSQLStatement(session, in.Statement)
	if err != nil {
		seq := atomic.AddInt64(&s.errorSequence, 1)
		log.Printf("failed to execute statement %v statement error sequence %d", err, seq)
		return fmt.Errorf("internal error in processing SQL statement. Please consult server logs for details. Statement Error sequence %d", seq)
	}

	// Have we switched schemas?
	if newSession != nil {
		session = newSession
		SetSession(stream.Context(), session)
		if executor == nil {
			return nil
		}
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
