package client

import (
	"context"
	"fmt"
	"github.com/squareup/pranadb/command/parser"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/squareup/pranadb/errors"

	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/protos/squareup/cash/pranadb/v1/service"
	"google.golang.org/grpc"
)

const maxBufferedLines = 1000

// Client is a simple client used for executing statements against PranaDB, it used by the CLI and elsewhere
type Client struct {
	lock             sync.Mutex
	started          bool
	serverAddress    string
	conn             *grpc.ClientConn
	client           service.PranaDBServiceClient
	currentStatement string
	pageSize         int
	currentSchema    string
}

func NewClient(serverAddress string) *Client {
	return &Client{
		serverAddress: serverAddress,
		pageSize:      10000,
	}
}

func (c *Client) Start() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.started {
		return nil
	}
	conn, err := grpc.Dial(c.serverAddress, grpc.WithInsecure())
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
	c.pageSize = pageSize
}

// ExecuteStatement executes a Prana statement. Lines of output will be received on the channel that is returned.
// When the channel is closed, the results are complete
func (c *Client) ExecuteStatement(statement string) (chan string, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if !c.started {
		return nil, errors.Error("not started")
	}
	if c.currentStatement != "" {
		return nil, errors.Errorf("statement currently executing: %s", c.currentStatement)
	}
	ch := make(chan string, maxBufferedLines)
	c.currentStatement = statement
	go c.doExecuteStatement(statement, ch)
	return ch, nil
}

func (c *Client) sendErrorToChannel(ch chan string, err error) {
	ch <- err.Error()
}

func (c *Client) doExecuteStatement(statement string, ch chan string) {
	if rc, err := c.doExecuteStatementWithError(statement, ch); err != nil {
		c.sendErrorToChannel(ch, err)
	} else {
		ch <- fmt.Sprintf("%d rows returned", rc)
	}
	c.lock.Lock()
	c.currentStatement = ""
	close(ch)
	c.lock.Unlock()
}

func (c *Client) doExecuteStatementWithError(statement string, ch chan string) (int, error) { //nolint:gocyclo

	ast, err := parser.Parse(statement)
	if err != nil {
		return 0, errors.Errorf("Failed to execute statement: %s", errors.NewInvalidStatementError(err.Error()).Error())
	}
	if ast.Use != "" {
		c.currentSchema = ast.Use
		return 0, nil
	}
	if c.currentSchema == "" && !(ast.Show != nil && ast.Show.Schemas != "") {
		return 0, errors.NewSchemaNotInUseError()
	}
	utc, _ := time.LoadLocation("UTC")

	stream, err := c.client.ExecuteSQLStatement(context.Background(), &service.ExecuteSQLStatementRequest{
		Schema:    c.currentSchema,
		Statement: statement,
		PageSize:  int32(c.pageSize),
	})
	if err != nil {
		return 0, errors.WithStack(err)
	}

	// Receive column metadata and page data until the result of the query is fully returned.
	var (
		columnNames []string
		columnTypes []common.ColumnType
		rowCount    = 0
	)
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			return 0, stripgRPCPrefix(err)
		}
		switch result := resp.Result.(type) {
		case *service.ExecuteSQLStatementResponse_Columns:
			columnNames, columnTypes = toColumnTypes(result.Columns)
			if len(columnTypes) != 0 {
				ch <- "|" + strings.Join(columnNames, "|") + "|"
			}

		case *service.ExecuteSQLStatementResponse_Page:
			if columnTypes == nil {
				return 0, errors.New("out of order response from server - column definitions should be first package not page data")
			}
			page := result.Page
			for _, row := range page.Rows {
				values := row.Values
				sb := strings.Builder{}
				sb.WriteRune('|')
				for colIndex, colType := range columnTypes {
					value := values[colIndex]
					if value.GetIsNull() {
						sb.WriteString("null|")
					} else {
						var sc string
						switch colType.Type {
						case common.TypeVarchar:
							sc = value.GetStringValue()
						case common.TypeTinyInt, common.TypeBigInt, common.TypeInt:
							sc = fmt.Sprintf("%d", value.GetIntValue())
						case common.TypeDecimal:
							sc = value.GetStringValue()
						case common.TypeDouble:
							sc = fmt.Sprintf("%g", value.GetFloatValue())
						case common.TypeTimestamp:
							unixTime := value.GetIntValue()
							gt := time.UnixMicro(unixTime).In(utc)
							sc = fmt.Sprintf("%d-%02d-%02d %02d:%02d:%02d.%06d",
								gt.Year(), gt.Month(), gt.Day(), gt.Hour(), gt.Minute(), gt.Second(), gt.Nanosecond()/1000)
						case common.TypeUnknown:
							sc = "??"
						}
						sb.WriteString(sc)
						sb.WriteRune('|')
					}
				}
				ch <- sb.String()
				rowCount++
			}
		}
	}
	return rowCount, nil
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

func stripgRPCPrefix(err error) error {
	// Strip out the gRPC internal crap from the error message
	ind := strings.Index(err.Error(), "PDB")
	if ind != -1 {
		msg := err.Error()[ind:]
		//Error string needs to be capitalized as this is what is displayed to the user in the CLI
		//nolint:stylecheck
		return errors.Errorf("Failed to execute statement: %s", msg)
	}
	return errors.WithStack(err)
}
