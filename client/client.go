package client

import (
	"context"
	"fmt"
	"io"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/squareup/pranadb/errors"

	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/protos/squareup/cash/pranadb/v1/service"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

const maxBufferedLines = 1000

// Client is a simple client used for executing statements against PranaDB, it used by the CLI and elsewhere
type Client struct {
	lock                  sync.Mutex
	started               bool
	serverAddress         string
	conn                  *grpc.ClientConn
	client                service.PranaDBServiceClient
	currentStatement      string
	sessionIDs            map[string]struct{}
	heartbeatTimer        *time.Timer
	heartbeatSendInterval time.Duration
	pageSize              int
}

func NewClient(serverAddress string, heartbeatSendInterval time.Duration) *Client {
	return &Client{
		serverAddress:         serverAddress,
		heartbeatSendInterval: heartbeatSendInterval,
		pageSize:              10000,
	}
}

func (c *Client) Start() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.started {
		return nil
	}
	c.sessionIDs = make(map[string]struct{})
	conn, err := grpc.Dial(c.serverAddress, grpc.WithInsecure())
	if err != nil {
		return errors.WithStack(err)
	}
	c.conn = conn
	c.client = service.NewPranaDBServiceClient(conn)
	c.scheduleHeartbeats()
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
	if c.heartbeatTimer != nil {
		c.heartbeatTimer.Stop()
	}
	return c.conn.Close()
}

func (c *Client) SetPageSize(pageSize int) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.pageSize = pageSize
}

func (c *Client) CreateSession() (string, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if !c.started {
		return "", errors.Error("not started")
	}
	if c.currentStatement != "" {
		return "", errors.Errorf("statement currently executing: %s", c.currentStatement)
	}
	resp, err := c.client.CreateSession(context.Background(), &emptypb.Empty{})
	if err != nil {
		return "", errors.WithStack(err)
	}
	sessID := resp.GetSessionId()
	c.sessionIDs[sessID] = struct{}{}
	return sessID, nil
}

func (c *Client) CloseSession(sessionID string) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if !c.started {
		return errors.Error("not started")
	}
	if c.currentStatement != "" {
		return errors.Errorf("statement currently executing: %s", c.currentStatement)
	}
	_, err := c.client.CloseSession(context.Background(), &service.CloseSessionRequest{SessionId: sessionID})
	delete(c.sessionIDs, sessionID)
	return errors.WithStack(err)
}

// ExecuteStatement executes a Prana statement. Lines of output will be received on the channel that is returned.
// When the channel is closed, the results are complete
func (c *Client) ExecuteStatement(sessionID string, statement string) (chan string, error) {
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
	go c.doExecuteStatement(sessionID, statement, ch)
	return ch, nil
}

func (c *Client) sendErrorToChannel(ch chan string, err error) {
	ch <- err.Error()
}

func (c *Client) doExecuteStatement(sessionID string, statement string, ch chan string) {
	if rc, err := c.doExecuteStatementWithError(sessionID, statement, ch); err != nil {
		c.sendErrorToChannel(ch, err)
	} else {
		ch <- fmt.Sprintf("%d rows returned", rc)
	}
	c.lock.Lock()
	c.currentStatement = ""
	close(ch)
	c.lock.Unlock()
}

func (c *Client) doExecuteStatementWithError(sessionID string, statement string, ch chan string) (int, error) {

	utc, _ := time.LoadLocation("UTC")

	stream, err := c.client.ExecuteSQLStatement(context.Background(), &service.ExecuteSQLStatementRequest{
		SessionId: sessionID,
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

func (c *Client) sendHeartbeats() {
	c.lock.Lock()
	defer c.lock.Unlock()
	if !c.started {
		return
	}
	for sessID := range c.sessionIDs {
		_, err := c.client.Heartbeat(context.Background(), &service.HeartbeatRequest{SessionId: sessID})
		if err != nil {
			err = stripgRPCPrefix(err)
			log.Errorf("heartbeat failed %+v", err)
			delete(c.sessionIDs, sessID)
		}
	}
	c.scheduleHeartbeats()
}

func (c *Client) scheduleHeartbeats() {
	c.heartbeatTimer = time.AfterFunc(c.heartbeatSendInterval, c.sendHeartbeats)
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

// used in testing
func (c *Client) disableHeartbeats() {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.heartbeatSendInterval = math.MaxInt64
	if c.heartbeatTimer != nil {
		c.heartbeatTimer.Stop()
	}
}
