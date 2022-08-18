package client

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/command/parser"
	"golang.org/x/net/http2"
	"google.golang.org/protobuf/types/descriptorpb"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/squareup/pranadb/errors"

	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/protos/squareup/cash/pranadb/v1/service"
	"google.golang.org/grpc"
)

const (
	maxBufferedLines     = 1000
	defaultMaxLineWidth  = 120
	minLineWidth         = 10
	minColWidth          = 5
	maxLineWidthPropName = "max_line_width"
	maxLineWidth         = 10000
)

// Client is a simple Client used for executing statements against PranaDB, it used by the CLI and elsewhere
// It can use the PranaDB gRPC or HTTP API for talking to PranaDB
type Client struct {
	lock                    sync.Mutex
	started                 bool
	serverAddress           string
	grpcConn                *grpc.ClientConn
	grpcClient              service.PranaDBServiceClient
	httpClient              *http.Client
	batchSize               int
	currentSchema           string
	maxLineWidth            int
	useHTTPAPI              bool
	disableCertVerification bool
	serverCertPath          string
}

func NewClientUsingGRPC(serverAddress string) *Client {
	return &Client{
		serverAddress: serverAddress,
		batchSize:     10000,
		maxLineWidth:  defaultMaxLineWidth,
	}
}

func NewClientUsingHTTP(serverAddress string, serverCertPath string) *Client {
	return &Client{
		serverAddress:  serverAddress,
		batchSize:      10000,
		maxLineWidth:   defaultMaxLineWidth,
		useHTTPAPI:     true,
		serverCertPath: serverCertPath,
	}
}

func (c *Client) Start() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.started {
		return nil
	}
	if c.useHTTPAPI {
		if c.serverCertPath == "" {
			return errors.New("no server certificate has been provided")
		}
		caCert, err := ioutil.ReadFile(c.serverCertPath)
		if err != nil {
			return errors.Errorf("failed to open server cert file: %s %v", c.serverCertPath, err)
		}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)
		tlsConfig := &tls.Config{
			RootCAs:            caCertPool,
			InsecureSkipVerify: c.disableCertVerification, //nolint:gosec
		}
		c.httpClient = &http.Client{}
		c.httpClient.Transport = &http2.Transport{
			TLSClientConfig: tlsConfig,
		}
	} else {
		conn, err := grpc.Dial(c.serverAddress, grpc.WithInsecure())
		if err != nil {
			return errors.WithStack(err)
		}
		c.grpcConn = conn
		c.grpcClient = service.NewPranaDBServiceClient(conn)
	}
	c.started = true
	return nil
}

func (c *Client) Stop() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if !c.started {
		return nil
	}
	c.started = false
	if c.useHTTPAPI {
		c.httpClient.CloseIdleConnections()
		return nil
	}
	return c.grpcConn.Close()
}

func (c *Client) SetDisableCertVerification(disable bool) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.disableCertVerification = disable
}

func (c *Client) SetPageSize(pageSize int) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.batchSize = pageSize
}

// ExecuteStatement executes a Prana statement. Lines of output will be received on the channel that is returned.
// When the channel is closed, the results are complete
func (c *Client) ExecuteStatement(statement string, argTypes []string, args []string) (chan string, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if !c.started {
		return nil, errors.Error("not started")
	}
	if strings.HasSuffix(statement, ";") {
		statement = statement[:len(statement)-1]
	}
	ch := make(chan string, maxBufferedLines)
	go c.doExecuteStatement(statement, argTypes, args, ch)
	return ch, nil
}

func (c *Client) sendErrorToChannel(ch chan string, errMsg string) {
	ch <- fmt.Sprintf("Failed to execute statement: %s", errMsg)
}

func (c *Client) doExecuteStatement(statement string, argTypes []string, args []string, ch chan string) {
	if rc, err := c.doExecuteStatementWithError(statement, argTypes, args, ch); err != nil {
		c.sendErrorToChannel(ch, err.Error())
	} else {
		ch <- fmt.Sprintf("%d rows returned", rc)
	}
	close(ch)
}

func (c *Client) handleSetCommand(statement string) error {
	parts := strings.Split(statement, " ")
	if len(parts) != 3 {
		return errors.Error("Invalid set command. Should be set <prop_name> <prop_value>")
	}
	if propName := parts[1]; propName == maxLineWidthPropName {
		propVal := parts[2]
		width, err := strconv.Atoi(propVal)
		if err != nil || width < minLineWidth || width > maxLineWidth {
			return errors.Errorf("Invalid %s value: %s", maxLineWidthPropName, propVal)
		}
		c.lock.Lock()
		defer c.lock.Unlock()
		c.maxLineWidth = width
	} else {
		return errors.Errorf("Unknown property: %s", propName)
	}
	return nil
}

func (c *Client) doExecuteStatementWithError(statement string, argTypes []string, args []string, ch chan string) (int, error) {
	lowerStat := strings.ToLower(statement)
	if lowerStat == "set" || strings.HasPrefix(strings.ToLower(lowerStat), "set ") {
		return 0, c.handleSetCommand(lowerStat)
	}
	ast, err := parser.Parse(statement)
	if err != nil {
		return 0, errors.NewInvalidStatementError(err.Error())
	}
	if ast.Use != "" {
		c.currentSchema = strings.ToLower(ast.Use)
		return 0, nil
	}
	if c.currentSchema == "" && !(ast.Show != nil && ast.Show.Schemas) {
		return 0, errors.NewSchemaNotInUseError()
	}
	if c.useHTTPAPI {
		return c.executeWithHTTPClient(statement, argTypes, args, ch)
	}
	return c.executeWithGRPClient(statement, argTypes, args, ch)
}

func (c *Client) executeWithHTTPClient(statement string, argTypes []string, args []string, ch chan string) (int, error) {
	statement = url.QueryEscape(statement)
	schema := url.QueryEscape(c.currentSchema)
	var argsParams string
	if len(argTypes) > 0 {
		quotedArgTypes := common.CSVQuote(argTypes...)
		argTypesParam := url.QueryEscape(quotedArgTypes)
		quotedArgs := common.CSVQuote(args...)
		argsParam := url.QueryEscape(quotedArgs)
		argsParams = fmt.Sprintf("&argtypes=%s&args=%s", argTypesParam, argsParam)
	}
	uri := fmt.Sprintf("https://%s/pranadb?stmt=%s&schema=%s&colheaders=true%s",
		c.serverAddress, statement, schema, argsParams)
	resp, err := c.httpClient.Get(uri) //nolint:bodyclose
	if err != nil {
		return 0, err
	}
	defer closeResponseBody(resp)
	if resp.StatusCode != http.StatusOK {
		bodyBytes, err := io.ReadAll(resp.Body)
		if err != nil {
			return 0, err
		}
		bodyString := string(bodyBytes)
		bodyString = bodyString[:len(bodyString)-1] // remove trailing \n
		return 0, errors.New(bodyString)
	}
	var (
		columnNames  []string
		columnTypes  []common.ColumnType
		columnWidths []int
		rowCount     = 0
		headerBorder string
		rowArr       []interface{}
	)
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := scanner.Text()
		if rowCount == 0 {
			// First line is column names
			colNames, err := toArray(line)
			if err != nil {
				return 0, err
			}
			columnNames = make([]string, len(colNames))
			for i, cn := range colNames {
				columnNames[i] = cn.(string) //nolint:forcetypeassert
			}
		} else if rowCount == 1 {
			// Second line is column types
			colTypes, err := toArray(line)
			if err != nil {
				return 0, err
			}
			columnTypes = make([]common.ColumnType, len(colTypes))
			for i, ct := range colTypes {
				sct := ct.(string) //nolint:forcetypeassert
				colType, err := common.StringToColumnType(sct)
				if err != nil {
					return 0, err
				}
				columnTypes[i] = colType
			}
			rowArr = make([]interface{}, len(columnTypes))

			// Now we write out the column header
			columnWidths = c.calcColumnWidths(columnTypes, columnNames)
			header := writeHeader(columnNames, columnWidths)
			headerBorder = createHeaderBorder(len(header))
			ch <- headerBorder
			ch <- header
			ch <- headerBorder
		} else {
			for i := range rowArr {
				if rowArr[i] == nil {
					rowArr[i] = &stringValUnmarshaller{}
				}
			}
			err := json.Unmarshal([]byte(line), &rowArr)
			if err != nil {
				return 0, err
			}
			valGetter := &httpValueGetter{
				colTypes: columnTypes,
				values:   rowArr,
			}
			line, err := formatLine(valGetter, columnWidths)
			if err != nil {
				return 0, err
			}
			ch <- line
		}
		rowCount++
	}
	if rowCount >= 2 {
		rowCount -= 2
		if rowCount > 0 {
			ch <- headerBorder
		}
	}
	return rowCount, nil
}

var _ json.Unmarshaler = &stringValUnmarshaller{}

type stringValUnmarshaller struct {
	strVal string
}

func (s *stringValUnmarshaller) UnmarshalJSON(bytes []byte) error {
	sv := string(bytes)
	if sv[0] == '"' {
		s.strVal = sv[1 : len(sv)-1] // it's a string value - strip quotes
	} else {
		s.strVal = sv
	}
	return nil
}

func (c *Client) executeWithGRPClient(statement string, argTypes []string, args []string, ch chan string) (int, error) { //nolint:gocyclo
	psArgs := make([]*service.Arg, len(args))
	for i, sargType := range argTypes {
		sarg := args[i]
		colType, err := common.StringToColumnType(sargType)
		if err != nil {
			return 0, err
		}
		var psArg *service.Arg
		switch colType.Type {
		case common.TypeTinyInt:
			val, err := strconv.ParseInt(sarg, 10, 64)
			if err != nil {
				return 0, err
			}
			psArg = &service.Arg{
				Type:  service.ColumnType_COLUMN_TYPE_TINY_INT,
				Value: &service.ArgValue{Value: &service.ArgValue_IntValue{IntValue: val}},
			}
		case common.TypeInt:
			val, err := strconv.ParseInt(sarg, 10, 64)
			if err != nil {
				return 0, err
			}
			psArg = &service.Arg{
				Type:  service.ColumnType_COLUMN_TYPE_INT,
				Value: &service.ArgValue{Value: &service.ArgValue_IntValue{IntValue: val}},
			}
		case common.TypeBigInt:
			val, err := strconv.ParseInt(sarg, 10, 64)
			if err != nil {
				return 0, err
			}
			psArg = &service.Arg{
				Type:  service.ColumnType_COLUMN_TYPE_BIG_INT,
				Value: &service.ArgValue{Value: &service.ArgValue_IntValue{IntValue: val}},
			}
		case common.TypeDouble:
			val, err := strconv.ParseFloat(sarg, 64)
			if err != nil {
				return 0, err
			}
			psArg = &service.Arg{
				Type:  service.ColumnType_COLUMN_TYPE_DOUBLE,
				Value: &service.ArgValue{Value: &service.ArgValue_FloatValue{FloatValue: val}},
			}
		case common.TypeVarchar:
			psArg = &service.Arg{
				Type:  service.ColumnType_COLUMN_TYPE_VARCHAR,
				Value: &service.ArgValue{Value: &service.ArgValue_StringValue{StringValue: sarg}},
			}
		case common.TypeDecimal:
			decParams := &service.DecimalParams{DecimalScale: uint32(colType.DecScale), DecimalPrecision: uint32(colType.DecPrecision)}
			psArg = &service.Arg{
				Type:          service.ColumnType_COLUMN_TYPE_DECIMAL,
				Value:         &service.ArgValue{Value: &service.ArgValue_DecimalValue{DecimalValue: sarg}},
				DecimalParams: decParams,
			}
		case common.TypeTimestamp:
			psArg = &service.Arg{
				Type:            service.ColumnType_COLUMN_TYPE_TIMESTAMP,
				Value:           &service.ArgValue{Value: &service.ArgValue_TimestampValue{TimestampValue: sarg}},
				TimestampParams: &service.TimestampParams{FractionalSecondsPrecision: uint32(colType.FSP)},
			}
		default:
			panic("unexpected col type")
		}
		psArgs[i] = psArg
	}

	stream, err := c.grpcClient.ExecuteStatement(context.Background(), &service.ExecuteStatementRequest{
		Schema:    c.currentSchema,
		Statement: &service.ExecuteStatementRequest_Sql{statement},
		BatchSize: int32(c.batchSize),
		Args:      psArgs,
	})
	if err != nil {
		return 0, checkErrorAndMaybeExit(err)
	}

	// Receive column metadata and page data until the result of the query is fully returned.
	var (
		columnNames  []string
		columnTypes  []common.ColumnType
		columnWidths []int
		rowCount     = 0
		wroteHeader  = false
		headerBorder string
	)
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			if rowCount > 0 {
				ch <- headerBorder
			}
			break
		} else if err != nil {
			return 0, checkErrorAndMaybeExit(err)
		}
		switch result := resp.Result.(type) {
		case *service.ExecuteStatementResponse_Columns:
			if !wroteHeader {
				columnNames, columnTypes = toColumnTypes(result.Columns)
				if len(columnTypes) > 0 {
					columnWidths = c.calcColumnWidths(columnTypes, columnNames)
					header := writeHeader(columnNames, columnWidths)
					headerBorder = createHeaderBorder(len(header))
					wroteHeader = true
					ch <- headerBorder
					ch <- header
					ch <- headerBorder
				}
			}
		case *service.ExecuteStatementResponse_Page:
			if columnTypes == nil {
				return 0, errors.New("out of order response from server - column definitions should be first package not page data")
			}
			page := result.Page
			for _, row := range page.Rows {
				values := row.Values
				valGetter := &grpcValueGetter{
					colTypes: columnTypes,
					values:   values,
				}
				line, err := formatLine(valGetter, columnWidths)
				if err != nil {
					return 0, err
				}
				ch <- line
				rowCount++
			}
		}
	}
	return rowCount, nil
}

func writeHeader(columnNames []string, columnWidths []int) string {
	sb := &strings.Builder{}
	sb.WriteString("|")
	for i, v := range columnNames {
		sb.WriteRune(' ')
		cw := columnWidths[i]
		if len(v) > cw {
			v = v[:cw-2] + ".."
		}
		sb.WriteString(rightPadToWidth(cw, v))
		sb.WriteString(" |")
	}
	return sb.String()
}

func createHeaderBorder(headerLen int) string {
	return "+" + strings.Repeat("-", headerLen-2) + "+"
}

func rightPadToWidth(width int, s string) string {
	padSpaces := width - len(s)
	pad := strings.Repeat(" ", padSpaces)
	s += pad
	return s
}

type valueGetter interface {
	isNull(colIndex int) bool
	getValueAsString(colIndex int) (string, error)
	numValues() int
}

type httpValueGetter struct {
	colTypes []common.ColumnType
	values   []interface{}
}

func (g *httpValueGetter) isNull(colIndex int) bool {
	return g.values[colIndex] == nil
}

func (g *httpValueGetter) numValues() int {
	return len(g.values)
}

func (g *httpValueGetter) getValueAsString(colIndex int) (string, error) {
	ct := g.colTypes[colIndex]
	value := g.values[colIndex]
	var strVal string
	switch ct.Type {
	case common.TypeVarchar, common.TypeTinyInt, common.TypeBigInt, common.TypeInt, common.TypeDecimal:
		um := value.(*stringValUnmarshaller) //nolint:forcetypeassert
		strVal = um.strVal
	case common.TypeDouble:
		um := value.(*stringValUnmarshaller) //nolint:forcetypeassert
		sv := um.strVal
		// We need to convert it to a float64 and back to a string as the string rep of the incoming float might be
		// in exponent form e.g. "1.23456e+123" but we don't want to display it in the cli with an exponent
		fv, err := strconv.ParseFloat(sv, 64)
		if err != nil {
			return "", err
		}
		strVal = strconv.FormatFloat(fv, 'f', -1, 64)
	case common.TypeTimestamp:
		um := value.(*stringValUnmarshaller) //nolint:forcetypeassert
		sval := um.strVal
		unixTime, err := strconv.ParseInt(sval, 10, 64)
		if err != nil {
			return "", err
		}
		strVal = convertUnixMicrosToDateString(unixTime)
	case common.TypeUnknown:
		panic("unknown col type")
	}
	return strVal, nil
}

type grpcValueGetter struct {
	colTypes []common.ColumnType
	values   []*service.ColValue
}

func (g *grpcValueGetter) isNull(colIndex int) bool {
	return g.values[colIndex].GetIsNull()
}

func (g *grpcValueGetter) numValues() int {
	return len(g.values)
}

func (g *grpcValueGetter) getValueAsString(colIndex int) (string, error) {
	ct := g.colTypes[colIndex]
	value := g.values[colIndex]
	var strVal string
	switch ct.Type {
	case common.TypeVarchar:
		strVal = value.GetStringValue()
	case common.TypeTinyInt, common.TypeBigInt, common.TypeInt:
		strVal = strconv.FormatInt(value.GetIntValue(), 10)
	case common.TypeDecimal:
		strVal = value.GetStringValue()
	case common.TypeDouble:
		strVal = strconv.FormatFloat(value.GetFloatValue(), 'f', -1, 64)
	case common.TypeTimestamp:
		unixTime := value.GetIntValue()
		strVal = convertUnixMicrosToDateString(unixTime)
	case common.TypeUnknown:
		panic("unknown coltype")
	}
	return strVal, nil
}

func convertUnixMicrosToDateString(unixTime int64) string {
	gt := time.UnixMicro(unixTime).In(time.UTC)
	return fmt.Sprintf("%d-%02d-%02d %02d:%02d:%02d.%06d",
		gt.Year(), gt.Month(), gt.Day(), gt.Hour(), gt.Minute(), gt.Second(), gt.Nanosecond()/1000)
}

func formatLine(valGetter valueGetter, colWidths []int) (string, error) {
	sb := &strings.Builder{}
	sb.WriteString("|")
	for i := 0; i < valGetter.numValues(); i++ {
		sb.WriteRune(' ')
		var v string
		if valGetter.isNull(i) {
			v = "null"
		} else {
			var err error
			v, err = valGetter.getValueAsString(i)
			if err != nil {
				return "", err
			}
		}
		cw := colWidths[i]
		if len(v) > cw {
			v = v[:cw-2] + ".."
		}
		sb.WriteString(rightPadToWidth(cw, v))
		sb.WriteString(" |")
	}
	return sb.String(), nil
}

func (c *Client) calcColumnWidth(numCols int) int {
	if numCols == 0 {
		return 0
	}
	colWidth := (c.maxLineWidth - 3*numCols - 1) / numCols
	if colWidth < minColWidth {
		colWidth = minColWidth
	}
	return colWidth
}

func (c *Client) calcColumnWidths(colTypes []common.ColumnType, colNames []string) []int {
	l := len(colTypes)
	if l == 0 {
		return []int{}
	}
	colWidths := make([]int, l)
	var freeCols []int
	availWidth := c.maxLineWidth - 1
	// We try to give the full col width to any cols with a fixed max size
	for i, colType := range colTypes {
		w := 0
		switch colType.Type {
		case common.TypeTinyInt:
			w = 4
		case common.TypeInt:
			w = 11
		case common.TypeBigInt:
			w = 20
		case common.TypeTimestamp:
			w = 26
		case common.TypeVarchar, common.TypeDecimal, common.TypeDouble:
			// We consider these free columns
			freeCols = append(freeCols, i)
		default:
		}
		if w != 0 {
			if len(colNames[i]) > w {
				w = len(colNames[i])
			}
			colWidths[i] = w
			availWidth -= w + 3
			if availWidth < 0 {
				break
			}
		}
	}
	if availWidth < 0 {
		// Fall back to just splitting up all columns evenly
		return c.calcEvenColWidths(l)
	} else if len(freeCols) > 0 {
		// For each free column we give it an equal share of what is remaining
		freeColWidth := (availWidth / len(freeCols)) - 3
		if freeColWidth < minColWidth {
			// Fall back to just splitting up all columns evenly
			return c.calcEvenColWidths(l)
		}
		for _, freeCol := range freeCols {
			colWidths[freeCol] = freeColWidth
		}
	}

	return colWidths
}

func (c *Client) calcEvenColWidths(numCols int) []int {
	colWidth := (c.maxLineWidth - 3*numCols - 1) / numCols
	if colWidth < minColWidth {
		colWidth = minColWidth
	}
	colWidths := make([]int, numCols)
	for i := range colWidths {
		colWidths[i] = colWidth
	}
	return colWidths
}

func toColumnTypes(result *service.Columns) (names []string, types []common.ColumnType) {
	types = make([]common.ColumnType, len(result.Columns))
	names = make([]string, len(result.Columns))
	for i, in := range result.Columns {
		columnType := common.ColumnType{
			Type: common.Type(in.Type),
		}
		if in.Type == service.ColumnType_COLUMN_TYPE_DECIMAL {
			if params := in.DecimalParams; params != nil {
				columnType.DecScale = int(params.DecimalScale)
				columnType.DecPrecision = int(params.DecimalPrecision)
			}
		}
		types[i] = columnType
		names[i] = in.Name
	}
	return
}

func closeResponseBody(resp *http.Response) {
	if err := resp.Body.Close(); err != nil {
		log.Errorf("failed to close http response %v", err)
	}
}

func (c *Client) RegisterProtobufs(ctx context.Context, descriptorSet *descriptorpb.FileDescriptorSet) error {
	if c.useHTTPAPI {
		buff := proto.NewBuffer(nil)
		if err := buff.Marshal(descriptorSet); err != nil {
			return err
		}
		b := buff.Bytes()
		uri := fmt.Sprintf("https://%s/pranadb/protoupload", c.serverAddress)
		resp, err := c.httpClient.Post(uri, "application/octet-stream", bytes.NewReader(b)) //nolint:bodyclose
		if err != nil {
			return err
		}
		defer closeResponseBody(resp)
		bodyBytes, err := io.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		if resp.StatusCode != http.StatusOK {
			return errors.Errorf("upload protos returned status %d with message %s", resp.StatusCode, string(bodyBytes))
		}
		return nil
	}
	req := &service.RegisterProtobufsRequest{Descriptors: descriptorSet}
	_, err := c.grpcClient.RegisterProtobufs(ctx, req)
	return errors.WithStack(err)
}

func checkErrorAndMaybeExit(err error) error {
	stripped := stripgRPCPrefix(err)
	if strings.HasPrefix(stripped.Error(), "PDB") {
		// It's a Prana error
		return stripped
	}
	log.Errorf("Connection error. Will exit. %v", stripped)
	os.Exit(1)
	return nil
}

func stripgRPCPrefix(err error) error {
	// Strip out the gRPC internal crap from the error message
	ind := strings.Index(err.Error(), "PDB")
	if ind != -1 {
		msg := err.Error()[ind:]
		return errors.Error(msg)
	}
	grpcCrap := "rpc error: code = Unavailable desc = connection error: desc = \""
	ind = strings.Index(err.Error(), grpcCrap)
	if ind != -1 {
		str := err.Error()
		msg := str[ind+len(grpcCrap) : len(str)-1]
		return errors.Error(msg)
	}
	return errors.WithStack(err)
}

func toArray(line string) ([]interface{}, error) {
	var arr []interface{}
	err := json.Unmarshal([]byte(line), &arr)
	return arr, err
}
