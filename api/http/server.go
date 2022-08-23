package http

import (
	"context"
	"encoding/csv"
	"fmt"
	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/api"
	"github.com/squareup/pranadb/command/parser"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/conf"
	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/execctx"
	"github.com/squareup/pranadb/meta"
	"github.com/squareup/pranadb/protolib"
	"github.com/squareup/pranadb/pull/exec"
	"google.golang.org/protobuf/types/descriptorpb"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

type HTTPAPIServer struct {
	listenAddress   string
	apiPath         string
	protoUploadPath string
	listener        net.Listener
	httpServer      *http.Server
	closeWg         sync.WaitGroup
	executor        sqlExecutor
	metaController  *meta.Controller
	tlsConf         conf.TLSConfig
	protoRegistry   *protolib.ProtoRegistry
}

type RowWriter interface {
	WriteRow(row *common.Row, writer http.ResponseWriter) error
	WriteContentType(writer http.ResponseWriter)
}

type sqlExecutor interface {
	ExecuteSQLStatement(execCtx *execctx.ExecutionContext, sql string, argTypes []common.ColumnType,
		args []interface{}) (exec.PullExecutor, error)
	CreateExecutionContext(ctx context.Context, schema *common.Schema) *execctx.ExecutionContext
}

const rowBatchSize = 1000

func NewHTTPAPIServer(listenAddress string, apiPath string, executor sqlExecutor, metaController *meta.Controller,
	protoRegistry *protolib.ProtoRegistry, tlsConf conf.TLSConfig) *HTTPAPIServer {
	return &HTTPAPIServer{
		listenAddress:   listenAddress,
		apiPath:         apiPath,
		protoUploadPath: fmt.Sprintf("%s/protoupload", apiPath),
		executor:        executor,
		metaController:  metaController,
		protoRegistry:   protoRegistry,
		tlsConf:         tlsConf,
	}
}

func (s *HTTPAPIServer) Start() error {
	tlsConf, err := api.CreateServerTLSConfig(s.tlsConf)
	if err != nil {
		return err
	}
	s.httpServer = &http.Server{
		Handler:     s,
		IdleTimeout: 0,
		TLSConfig:   tlsConf,
	}
	s.listener, err = net.Listen("tcp", s.listenAddress)
	if err != nil {
		return err
	}
	s.closeWg = sync.WaitGroup{}
	s.closeWg.Add(1)
	go func() {
		err = s.httpServer.ServeTLS(s.listener, "", "")
		if err != http.ErrServerClosed {
			log.Errorf("Failed to start the HTTP API server: %v", err)
		}
		s.closeWg.Done()
	}()
	return nil
}

func (s *HTTPAPIServer) Stop() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.httpServer.Shutdown(ctx); err != nil {
		return err
	}
	s.closeWg.Wait()
	return nil
}

func (s *HTTPAPIServer) ServeHTTP(writer http.ResponseWriter, request *http.Request) { //nolint:gocyclo
	defer common.PanicHandler()

	if request.ProtoMajor != 2 {
		http.Error(writer, "the pranadb HTTP API supports HTTP2 only", http.StatusHTTPVersionNotSupported)
		return
	}
	uri, err := url.ParseRequestURI(request.RequestURI)
	if err != nil {
		writer.WriteHeader(http.StatusBadRequest)
		return
	}
	if uri.Path == s.protoUploadPath {
		s.handleProtoUpload(writer, request)
		return
	}
	if uri.Path != s.apiPath {
		http.NotFound(writer, request)
		return
	}
	if request.Method != http.MethodGet {
		http.Error(writer, "the pranadb HTTP API only supports GET method", http.StatusMethodNotAllowed)
		return
	}
	query := uri.Query()
	statement := query.Get("stmt")
	if statement == "" {
		http.Error(writer, "missing 'stmt' query parameter", http.StatusBadRequest)
		return
	}
	schemaName := query.Get("schema")
	if schemaName == "" {
		ast, err := parser.Parse(statement)
		isShowSchemas := err == nil && ast.Show != nil && ast.Show.Schemas
		if !isShowSchemas {
			http.Error(writer, "missing 'schema' query parameter", http.StatusBadRequest)
			return
		}
	}
	includeHeader := false
	sincludeHeader := query.Get("colheaders")
	if sincludeHeader != "" && strings.ToLower(sincludeHeader) == "true" {
		includeHeader = true
	}
	argTypes, ok := parseArgTypes(writer, query)
	if !ok {
		return
	}
	var args []interface{}
	if argTypes != nil {
		args, ok = parseArgs(argTypes, writer, query)
		if !ok {
			return
		}
	}
	var schema *common.Schema
	if schemaName != "" {
		schema = s.metaController.GetOrCreateSchema(strings.ToLower(schemaName))
	}
	execCtx := s.executor.CreateExecutionContext(request.Context(), schema)
	defer func() {
		s.metaController.DeleteSchemaIfEmpty(schema)
	}()
	pe, err := s.executor.ExecuteSQLStatement(execCtx, statement, argTypes, args)
	if err != nil {
		maybeConvertAndSendError(err, writer)
		return
	}
	err = s.executeStatement(pe, writer, includeHeader)
	if err != nil {
		maybeConvertAndSendError(err, writer)
	}
}

func (s *HTTPAPIServer) executeStatement(pe exec.PullExecutor, writer http.ResponseWriter, includeHeader bool) error {
	rowWriter := &jsonLinesRowWriter{}
	rowWriter.WriteContentType(writer)
	for {
		rows, err := pe.GetRows(rowBatchSize)
		if err != nil {
			return err
		}
		if includeHeader {
			// Write the column header
			// We write column names, then column types
			if err := writeColumnHeaders(rowWriter, writer, pe.ColNames(), pe.ColTypes()); err != nil {
				return err
			}
		}
		for i := 0; i < rows.RowCount(); i++ {
			row := rows.GetRow(i)
			if err := rowWriter.WriteRow(&row, writer); err != nil {
				return err
			}
		}
		if rows.RowCount() < rowBatchSize {
			break
		}
	}
	return nil
}

func writeColumnHeaders(writer RowWriter, rw http.ResponseWriter, colNames []string, colTypes []common.ColumnType) error {
	headerCT := make([]common.ColumnType, len(colTypes))
	for i := range headerCT {
		headerCT[i] = common.VarcharColumnType
	}
	rf := common.NewRowsFactory(headerCT)
	rows := rf.NewRows(2)
	for i, colName := range colNames {
		rows.AppendStringToColumn(i, colName)
	}
	for i, colType := range colTypes {
		var ctName string
		switch colType.Type {
		case common.TypeTinyInt:
			ctName = "tinyint"
		case common.TypeInt:
			ctName = "int"
		case common.TypeBigInt:
			ctName = "bigint"
		case common.TypeDouble:
			ctName = "double"
		case common.TypeVarchar:
			ctName = "varchar"
		case common.TypeDecimal:
			ctName = fmt.Sprintf("decimal(%d,%d)", colType.DecPrecision, colType.DecScale)
		case common.TypeTimestamp:
			ctName = fmt.Sprintf("timestamp(%d)", colType.FSP)
		default:
			panic("unexpected col type")
		}
		rows.AppendStringToColumn(i, ctName)
	}
	for i := 0; i < rows.RowCount(); i++ {
		row := rows.GetRow(i)
		if err := writer.WriteRow(&row, rw); err != nil {
			return err
		}
	}
	return nil
}

func parseArgTypes(writer http.ResponseWriter, query url.Values) ([]common.ColumnType, bool) {
	sargTypes := query.Get("argtypes")
	if sargTypes == "" {
		return nil, true
	}
	reader := csv.NewReader(strings.NewReader(sargTypes))
	arr, err := reader.Read()
	if err != nil {
		http.Error(writer, fmt.Sprintf("invalid arg types %s", sargTypes), http.StatusBadRequest)
		return nil, false
	}
	argTypes := make([]common.ColumnType, len(arr))
	for i, sargtype := range arr {
		argType, err := common.StringToColumnType(sargtype)
		if err != nil {
			http.Error(writer, err.Error(), http.StatusBadRequest)
			return nil, false
		}
		argTypes[i] = argType
	}
	return argTypes, true
}

func parseArgs(argTypes []common.ColumnType, writer http.ResponseWriter, query url.Values) ([]interface{}, bool) {
	sargs := query.Get("args")
	if sargs == "" {
		return nil, true
	}
	reader := csv.NewReader(strings.NewReader(sargs))
	arr, err := reader.Read()
	if err != nil {
		http.Error(writer, fmt.Sprintf("invalid args %s", sargs), http.StatusBadRequest)
		return nil, false
	}
	if len(arr) != len(argTypes) {
		http.Error(writer, "number of argtypes does not match number of args", http.StatusBadRequest)
		return nil, false
	}
	args := make([]interface{}, len(arr))
	for i, argType := range argTypes {
		sarg := arr[i]
		switch argType.Type {
		case common.TypeTinyInt, common.TypeInt, common.TypeBigInt:
			val, err := strconv.ParseInt(sarg, 10, 64)
			if err != nil {
				http.Error(writer, fmt.Sprintf("invalid integer argument %s", sarg), http.StatusBadRequest)
				return nil, false
			}
			if argType.Type == common.TypeTinyInt && (val < -128 || val > 127) {
				http.Error(writer, fmt.Sprintf("tinyint value out of range %d", val), http.StatusBadRequest)
				return nil, false
			} else if argType.Type == common.TypeInt && (val < math.MinInt32 || val > math.MaxInt32) {
				http.Error(writer, fmt.Sprintf("int value out of range %d", val), http.StatusBadRequest)
				return nil, false
			}
			args[i] = val
		case common.TypeDouble:
			val, err := strconv.ParseFloat(sarg, 64)
			if err != nil {
				http.Error(writer, fmt.Sprintf("invalid double argument %s", sarg), http.StatusBadRequest)
				return nil, false
			}
			args[i] = val
		case common.TypeVarchar, common.TypeDecimal, common.TypeTimestamp:
			args[i] = sarg
		default:
			panic(fmt.Sprintf("unknown col type %d", argType.Type))
		}
	}
	return args, true
}

func (s *HTTPAPIServer) handleProtoUpload(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		http.Error(writer, "the pranadb proto upload HTTP API only supports POST method", http.StatusMethodNotAllowed)
		return
	}
	bytes, err := io.ReadAll(request.Body)
	if err != nil {
		maybeConvertAndSendError(err, writer)
		return
	}
	buf := proto.NewBuffer(bytes)
	descriptorSet := &descriptorpb.FileDescriptorSet{}
	if err := buf.Unmarshal(descriptorSet); err != nil {
		maybeConvertAndSendError(err, writer)
		return
	}
	if err := s.protoRegistry.RegisterFiles(descriptorSet); err != nil {
		maybeConvertAndSendError(err, writer)
		return
	}
}

func maybeConvertAndSendError(err error, writer http.ResponseWriter) {
	perr := api.MaybeConvertError(err)
	var statusCode int
	if perr.Code == errors.InternalError {
		statusCode = http.StatusInternalServerError
	} else {
		statusCode = http.StatusBadRequest
	}
	http.Error(writer, perr.Msg, statusCode)
}
