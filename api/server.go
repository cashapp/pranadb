package api

import (
	"context"
	"fmt"

	"net"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/squareup/pranadb/conf/tls"
	"github.com/squareup/pranadb/pull/exec"
	"google.golang.org/grpc/credentials"

	"github.com/squareup/pranadb/meta"

	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/command"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/conf"
	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/protolib"
	"github.com/squareup/pranadb/protos/squareup/cash/pranadb/v1/service"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/encoding/gzip" // Registers gzip (de)-compressor
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/types/known/emptypb"
)

var _ service.PranaDBServiceServer = &Server{}

type Server struct {
	lock           sync.Mutex
	started        bool
	ce             *command.Executor
	serverAddress  string
	gsrv           *grpc.Server
	errorSequence  int64
	protoRegistry  *protolib.ProtoRegistry
	metaController *meta.Controller
	tlsConfig      tls.TLSConfig
}

func NewAPIServer(metaController *meta.Controller, ce *command.Executor, protobufs *protolib.ProtoRegistry, cfg conf.Config) *Server {
	return &Server{
		metaController: metaController,
		ce:             ce,
		protoRegistry:  protobufs,
		serverAddress:  cfg.GRPCAPIServerListenAddresses[cfg.NodeID],
		tlsConfig:      cfg.APITLSConfig,
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
		return errors.WithStack(err)
	}
	opts, err := s.getTLSOpts()
	if err != nil {
		return errors.WithStack(err)
	}
	s.gsrv = grpc.NewServer(opts...)
	reflection.Register(s.gsrv)
	service.RegisterPranaDBServiceServer(s.gsrv, s)
	s.started = true
	go s.startServer(list)
	return nil
}

func (s *Server) getTLSOpts() ([]grpc.ServerOption, error) {
	if reflect.DeepEqual(s.tlsConfig, tls.TLSConfig{}) {
		return []grpc.ServerOption{}, nil
	}
	tlsConfig, err := tls.BuildTLSConfig(s.tlsConfig)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	opts := []grpc.ServerOption{
		grpc.Creds(credentials.NewTLS(tlsConfig)),
	}
	return opts, nil
}

func (s *Server) startServer(list net.Listener) {
	err := s.gsrv.Serve(list) //nolint:ifshort
	s.lock.Lock()
	defer s.lock.Unlock()
	s.started = false
	if err != nil {
		log.Errorf("grpc server listen failed: %v", err)
	}
}

func (s *Server) Stop() error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if !s.started {
		return nil
	}
	s.gsrv.Stop()
	s.errorSequence = 0
	return nil
}

func (s *Server) ExecuteStatement(in *service.ExecuteStatementRequest,
	stream service.PranaDBService_ExecuteStatementServer) error {
	defer common.PanicHandler()
	var schema *common.Schema
	if in.Schema != "" {
		schema = s.metaController.GetOrCreateSchema(strings.ToLower(in.Schema))
	}
	execCtx := s.ce.CreateExecutionContext(schema)
	defer func() {
		s.metaController.DeleteSchemaIfEmpty(schema)
	}()
	numArgs := len(in.Args)
	args := make([]interface{}, numArgs)
	argTypes := make([]common.ColumnType, numArgs)
	for i, arg := range in.Args {
		var argType common.ColumnType
		var argValue interface{}
		switch arg.Type {
		case service.ColumnType_COLUMN_TYPE_TINY_INT:
			argType = common.TinyIntColumnType
			argValue = arg.Value.GetIntValue()
		case service.ColumnType_COLUMN_TYPE_INT:
			argType = common.IntColumnType
			argValue = arg.Value.GetIntValue()
		case service.ColumnType_COLUMN_TYPE_BIG_INT:
			argType = common.BigIntColumnType
			argValue = arg.Value.GetIntValue()
		case service.ColumnType_COLUMN_TYPE_DOUBLE:
			argType = common.DoubleColumnType
			argValue = arg.Value.GetFloatValue()
		case service.ColumnType_COLUMN_TYPE_DECIMAL:
			argType = common.ColumnType{Type: common.TypeDecimal}
			dp := arg.GetDecimalParams()
			if dp != nil {
				argType.DecPrecision = int(dp.DecimalPrecision)
				argType.DecScale = int(dp.DecimalScale)
			} else {
				argType.DecPrecision = 65
				argType.DecScale = 30
			}
			argValue = arg.Value.GetDecimalValue()
		case service.ColumnType_COLUMN_TYPE_VARCHAR:
			argType = common.VarcharColumnType
			argValue = arg.Value.GetStringValue()
		case service.ColumnType_COLUMN_TYPE_TIMESTAMP:
			argType = common.ColumnType{Type: common.TypeTimestamp}
			argType.FSP = 6
			argValue = arg.Value.GetTimestampValue()
		default:
			return errors.Errorf("unexpected arg type %d", arg.Type)
		}
		argTypes[i] = argType
		args[i] = argValue
	}
	stmt := in.Statement
	sqlStmt, ok := stmt.(*service.ExecuteStatementRequest_Sql)
	if !ok {
		return errors.New("named statements currently not supported")
	}
	executor, err := s.ce.ExecuteSQLStatement(execCtx, sqlStmt.Sql, argTypes, args)
	return s.doExecuteStatement(executor, int(in.BatchSize), err, stream)
}

func (s *Server) doExecuteStatement(executor exec.PullExecutor, batchSize int, err error,
	stream service.PranaDBService_ExecuteStatementServer) error {
	if err != nil {
		return MaybeConvertError(err)
	}

	sentColHeaders := false

	// Then start sending pages until complete.
	numCols := len(executor.ColTypes())
	for {
		// Transcode rows.
		rows, err := executor.GetRows(batchSize)
		if err != nil {
			return errors.WithStack(err)
		}

		if !sentColHeaders {
			// First send column definitions - we do this AFTER sending get rows as we don't want to send back col headers
			// if an error in getRows occurs
			columns := &service.Columns{}
			names := executor.ColNames()
			for i, typ := range executor.ColTypes() {
				name := names[i]
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
			if err := stream.Send(&service.ExecuteStatementResponse{Result: &service.ExecuteStatementResponse_Columns{Columns: columns}}); err != nil {
				return errors.WithStack(err)
			}
			sentColHeaders = true
		}

		prows := make([]*service.Row, rows.RowCount())
		for i := 0; i < rows.RowCount(); i++ {
			row := rows.GetRow(i)
			colVals := make([]*service.ColValue, numCols)
			for colNum, colType := range executor.ColTypes() {
				colVal := &service.ColValue{}
				colVals[colNum] = colVal
				if row.IsNull(colNum) {
					colVal.Value = &service.ColValue_IsNull{IsNull: true}
				} else {
					switch colType.Type {
					case common.TypeTinyInt, common.TypeInt, common.TypeBigInt:
						colVal.Value = &service.ColValue_IntValue{IntValue: row.GetInt64(colNum)}
					case common.TypeDouble:
						colVal.Value = &service.ColValue_FloatValue{FloatValue: row.GetFloat64(colNum)}
					case common.TypeVarchar:
						colVal.Value = &service.ColValue_StringValue{StringValue: row.GetString(colNum)}
					case common.TypeDecimal:
						dec := row.GetDecimal(colNum)
						// We encode the decimal as a string
						colVal.Value = &service.ColValue_StringValue{StringValue: dec.String()}
					case common.TypeTimestamp:
						ts := row.GetTimestamp(colNum)
						gt, err := ts.GoTime(time.UTC)
						if err != nil {
							return err
						}
						// We encode a datetime as *microseconds* past epoch
						unixTime := gt.UnixNano() / 1000
						colVal.Value = &service.ColValue_IntValue{IntValue: unixTime}
					default:
						panic(fmt.Sprintf("unexpected column type %d", colType.Type))
					}
				}
			}
			pRow := &service.Row{Values: colVals}
			prows[i] = pRow
		}
		numRows := rows.RowCount()
		results := &service.Page{
			Count: uint64(numRows),
			Rows:  prows,
		}
		if err = stream.Send(&service.ExecuteStatementResponse{Result: &service.ExecuteStatementResponse_Page{Page: results}}); err != nil {
			return errors.WithStack(err)
		}
		if numRows < batchSize {
			break
		}
	}
	return nil
}

func (s *Server) RegisterProtobufs(ctx context.Context, request *service.RegisterProtobufsRequest) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, s.protoRegistry.RegisterFiles(request.GetDescriptors())
}

func (s *Server) GetListenAddress() string {
	return s.serverAddress
}
