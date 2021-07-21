package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/alecthomas/kong"
	"github.com/chzyer/readline"
	"github.com/pkg/errors"
	"google.golang.org/grpc"

	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/protos/squareup/cash/pranadb/v1/service"
)

var cli struct {
	Addr string `help:"Address of PranaDB server to connect to." default:"127.0.0.1:6584"`
	VI   bool   `help:"Enable VI mode."`
}

func main() {
	kctx := kong.Parse(&cli)
	conn, err := grpc.Dial(cli.Addr, grpc.WithInsecure())
	kctx.FatalIfErrorf(err)
	client := service.NewPranaDBServiceClient(conn)

	stream, err := client.ExecuteSQLStatement(context.Background())
	kctx.FatalIfErrorf(err)

	home, err := os.UserHomeDir()
	kctx.FatalIfErrorf(err)

	rl, err := readline.NewEx(&readline.Config{
		HistoryFile:            filepath.Join(home, ".prana.history"),
		DisableAutoSaveHistory: true,
		VimMode:                cli.VI,
	})
	kctx.FatalIfErrorf(err)
	for {
		// Gather multi-line SQL terminated by a ;
		rl.SetPrompt("pranadb> ")
		cmd := []string{}
		for {
			line, err := rl.Readline()
			if err == io.EOF {
				kctx.Exit(0)
			}
			kctx.FatalIfErrorf(err)
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			cmd = append(cmd, line)
			if strings.HasSuffix(line, ";") {
				break
			}
			rl.SetPrompt("         ")
		}
		sql := strings.Join(cmd, " ")
		_ = rl.SaveHistory(sql)

		err = sendStatement(stream, sql)
		kctx.FatalIfErrorf(err)
	}
}

func sendStatement(stream service.PranaDBService_ExecuteSQLStatementClient, sql string) error {
	// Send request.
	err := stream.Send(&service.ExecuteSQLStatementRequest{
		Query:    sql,
		PageSize: 1000,
	})
	if err != nil {
		return errors.WithStack(err)
	}

	// Receive column metadata and page data until the result of the query is fully returned.
	var (
		columnNames []string
		columnTypes []common.ColumnType
		rowsFactory *common.RowsFactory
		count       = 0
		more        = true
	)
	for more {
		resp, err := stream.Recv()
		if err != nil {
			return errors.WithStack(err)
		}
		switch result := resp.Result.(type) {
		case *service.ExecuteSQLStatementResponse_Columns:
			columnNames, columnTypes = toColumnTypes(result.Columns)
			if len(columnTypes) != 0 {
				fmt.Println(strings.Join(columnNames, "|"))
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
				fmt.Println(strings.Join(rowstr, "|"))
			}
			count += int(page.Count)
			more = page.More
			if !more {
				if count == 1 {
					fmt.Printf("1 row returned\n")
				} else {
					fmt.Printf("%d rows returned\n", count)
				}
			}

		case *service.ExecuteSQLStatementResponse_Error:
			fmt.Printf("error: %s\n", result.Error)
			more = false
		}
	}
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
