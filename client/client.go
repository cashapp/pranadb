package client

import (
	"context"
	"fmt"
	"io"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/command/parser"
	pranadbtls "github.com/squareup/pranadb/conf/tls"
	"google.golang.org/grpc/credentials"

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

// Client is a simple client used for executing statements against PranaDB, it used by the CLI and elsewhere
type Client struct {
	lock          sync.Mutex
	started       bool
	serverAddress string
	conn          *grpc.ClientConn
	client        service.PranaDBServiceClient
	batchSize     int
	currentSchema string
	maxLineWidth  int
	tlsConfig     pranadbtls.TLSConfig
}

func NewClient(serverAddress string, tlsConfig pranadbtls.TLSConfig) *Client {
	return &Client{
		serverAddress: serverAddress,
		batchSize:     10000,
		maxLineWidth:  defaultMaxLineWidth,
		tlsConfig:     tlsConfig,
	}
}

func (c *Client) Start() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.started {
		return nil
	}
	dialOpts, err := c.getDialOptions()
	if err != nil {
		return errors.WithStack(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, c.serverAddress, dialOpts)
	if err != nil {
		return errors.WithStack(err)
	}
	c.conn = conn
	c.client = service.NewPranaDBServiceClient(conn)
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
	return c.conn.Close()
}

func (c *Client) SetPageSize(pageSize int) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.batchSize = pageSize
}

// ExecuteStatement executes a Prana statement. Lines of output will be received on the channel that is returned.
// When the channel is closed, the results are complete
func (c *Client) ExecuteStatement(statement string, args []*service.Arg) (chan string, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if !c.started {
		return nil, errors.Error("not started")
	}
	if strings.HasSuffix(statement, ";") {
		statement = statement[:len(statement)-1]
	}
	ch := make(chan string, maxBufferedLines)
	go c.doExecuteStatement(statement, args, ch)
	return ch, nil
}

func (c *Client) sendErrorToChannel(ch chan string, err error) {
	ch <- fmt.Sprintf("Failed to execute statement: %s", err.Error())
}

func (c *Client) doExecuteStatement(statement string, args []*service.Arg, ch chan string) {
	if rc, err := c.doExecuteStatementWithError(statement, args, ch); err != nil {
		c.sendErrorToChannel(ch, err)
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

func (c *Client) doExecuteStatementWithError(statement string, args []*service.Arg, ch chan string) (int, error) {
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
	stream, err := c.client.ExecuteStatement(context.Background(), &service.ExecuteStatementRequest{
		Schema:    c.currentSchema,
		Statement: &service.ExecuteStatementRequest_Sql{statement},
		BatchSize: int32(c.batchSize),
		Args:      args,
	})
	if err != nil {
		return 0, checkErrorAndMaybeExit(err)
	}
	return c.readStream(stream, ch)
}

func (c *Client) readStream(stream service.PranaDBService_ExecuteStatementClient, ch chan string) (int, error) {
	// Receive column metadata and page data until the result of the query is fully returned.
	var (
		columnNames  []string
		columnTypes  []common.ColumnType
		columnWidths []int
		rowCount     = 0
		wroteHeader  = false
		headerLine   string
	)
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			if rowCount > 0 {
				ch <- headerLine
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
					sb := &strings.Builder{}
					sb.WriteString("|")
					totWidth := 0
					for i, v := range columnNames {
						sb.WriteRune(' ')
						cw := columnWidths[i]
						if len(v) > cw {
							v = v[:cw-2] + ".."
						}
						sb.WriteString(rightPadToWidth(cw, v))
						sb.WriteString(" |")
						totWidth += cw + 3
					}
					headerLine = "+" + strings.Repeat("-", totWidth-1) + "+"
					wroteHeader = true
					ch <- headerLine
					ch <- sb.String()
					ch <- headerLine
				}
			}
		case *service.ExecuteStatementResponse_Page:
			if columnTypes == nil {
				return 0, errors.New("out of order response from server - column definitions should be first package not page data")
			}
			page := result.Page
			for _, row := range page.Rows {
				values := row.Values
				line := formatLine(values, columnTypes, columnWidths)
				ch <- line
				rowCount++
			}
		}
	}
	return rowCount, nil
}

func rightPadToWidth(width int, s string) string {
	padSpaces := width - len(s)
	pad := strings.Repeat(" ", padSpaces)
	s += pad
	return s
}

func formatLine(values []*service.ColValue, colTypes []common.ColumnType, colWidths []int) string {
	sb := &strings.Builder{}
	sb.WriteString("|")
	for i, value := range values {
		sb.WriteRune(' ')
		colType := colTypes[i]
		var v string
		if value.GetIsNull() {
			v = "null"
		} else {
			switch colType.Type {
			case common.TypeVarchar:
				v = value.GetStringValue()
			case common.TypeTinyInt, common.TypeBigInt, common.TypeInt:
				v = fmt.Sprintf("%d", value.GetIntValue())
			case common.TypeDecimal:
				v = value.GetStringValue()
			case common.TypeDouble:
				v = fmt.Sprintf("%f", value.GetFloatValue())
			case common.TypeTimestamp:
				unixTime := value.GetIntValue()
				gt := time.UnixMicro(unixTime).In(time.UTC)
				v = fmt.Sprintf("%d-%02d-%02d %02d:%02d:%02d.%06d",
					gt.Year(), gt.Month(), gt.Day(), gt.Hour(), gt.Minute(), gt.Second(), gt.Nanosecond()/1000)
			case common.TypeUnknown:
				v = "??"
			}
		}
		cw := colWidths[i]
		if len(v) > cw {
			v = v[:cw-2] + ".."
		}
		sb.WriteString(rightPadToWidth(cw, v))
		sb.WriteString(" |")
	}
	return sb.String()
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

func (c *Client) RegisterProtobufs(ctx context.Context, in *service.RegisterProtobufsRequest, option ...grpc.CallOption) error {
	_, err := c.client.RegisterProtobufs(ctx, in, option...)
	return errors.WithStack(err)
}

func (c *Client) getDialOptions() (grpc.DialOption, error) {
	if reflect.DeepEqual(c.tlsConfig, pranadbtls.TLSConfig{}) {
		return grpc.WithInsecure(), nil
	}
	tlsConfig, err := pranadbtls.BuildTLSConfig(c.tlsConfig)
	if err != nil {
		return grpc.EmptyDialOption{}, errors.WithStack(err)
	}
	return grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)), nil
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
