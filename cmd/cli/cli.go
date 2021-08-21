package cli

import (
	"context"
	"errors"
	"fmt"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/protos/squareup/cash/pranadb/v1/service"
	"google.golang.org/grpc"
	"io"
	"strings"
	"sync"
)

const maxBufferedLines = 1000

type Cli struct {
	lock          sync.Mutex
	started       bool
	serverAddress string
	conn          *grpc.ClientConn
	client        service.PranaDBServiceClient
	executing     bool
}

func NewCli(serverAddress string) *Cli {
	return &Cli{
		serverAddress: serverAddress,
	}
}

func (c *Cli) Start() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.started {
		return nil
	}
	conn, err := grpc.Dial(c.serverAddress, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c.conn = conn
	c.client = service.NewPranaDBServiceClient(conn)
	c.started = true
	return nil
}

func (c *Cli) Stop() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if !c.started {
		return nil
	}
	if err := c.conn.Close(); err != nil {
		return err
	}
	c.started = false
	return nil
}

// ExecuteStatement executes a Prana statement. Lines of output will be received on the channel that is returned.
// When the channel is closed, the results are complete
func (c *Cli) ExecuteStatement(statement string) (chan string, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.executing {
		return nil, errors.New("statement already executing")
	}
	ch := make(chan string, maxBufferedLines)
	c.executing = true
	go c.doExecuteStatement(statement, ch)
	return ch, nil
}

func (c *Cli) sendErrorToChannel(ch chan string, err error) {
	msg := fmt.Sprintf("internal error: %v", err)
	ch <- msg
}

func (c *Cli) doExecuteStatement(statement string, ch chan string) {
	if err := c.doExecuteStatementWithError(statement, ch); err != nil {
		c.sendErrorToChannel(ch, err)
	}
	c.lock.Lock()
	c.executing = false
	c.lock.Unlock()
}

func (c *Cli) doExecuteStatementWithError(statement string, ch chan string) error {
	stream, err := c.client.ExecuteSQLStatement(context.Background(), &service.ExecuteSQLStatementRequest{
		Statement: statement,
		PageSize:  1000,
	})
	if err != nil {
		return err
	}

	// Receive column metadata and page data until the result of the query is fully returned.
	var (
		columnNames []string
		columnTypes []common.ColumnType
		rowsFactory *common.RowsFactory
		count       = 0
	)
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		switch result := resp.Result.(type) {
		case *service.ExecuteSQLStatementResponse_Columns:
			columnNames, columnTypes = toColumnTypes(result.Columns)
			if len(columnTypes) != 0 {
				ch <- strings.Join(columnNames, "|")
			}
			rowsFactory = common.NewRowsFactory(columnTypes)

		case *service.ExecuteSQLStatementResponse_Page:
			if rowsFactory == nil {
				return errors.New("out of order response from server - column definitions should be first package not page data")
			}
			page := result.Page
			rows := rowsFactory.NewRows(int(page.Count))
			rows.Deserialize(page.Rows)
			rowstr := make([]string, len(columnTypes))
			for ri := 0; ri < rows.RowCount(); ri++ {
				row := rows.GetRow(ri)
				for ci, ct := range rows.ColumnTypes() {
					switch ct.Type {
					case common.TypeVarchar:
						rowstr[ci] = row.GetString(ci)
					case common.TypeTinyInt, common.TypeBigInt, common.TypeInt:
						rowstr[ci] = fmt.Sprintf("%v", row.GetInt64(ci))
					case common.TypeDecimal:
						dec := row.GetDecimal(ci)
						rowstr[ci] = dec.String()
					case common.TypeDouble:
						rowstr[ci] = fmt.Sprintf("%g", row.GetFloat64(ci))
					case common.TypeTimestamp:
						rowstr[ci] = row.GetString(ci)
					case common.TypeUnknown:
						rowstr[ci] = "??"
					}
				}
				ch <- strings.Join(rowstr, "|")
			}
			count += int(page.Count)
		}
	}
	close(ch)
	return nil
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
