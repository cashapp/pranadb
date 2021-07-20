package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/alecthomas/kong"
	"github.com/chzyer/readline"
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
		err = stream.Send(&service.ExecuteSQLStatementRequest{Query: sql})
		kctx.FatalIfErrorf(err)
		resp, err := stream.Recv()
		kctx.FatalIfErrorf(err)
		switch result := resp.Result.(type) {
		case *service.ExecuteSQLStatementResponse_Results:
			names, types := toColumnTypes(result.Results)
			rowsf := common.NewRowsFactory(types)
			rows := rowsf.NewRows(int(result.Results.RowCount))
			rows.Deserialize(result.Results.Rows)
			if result.Results.RowCount != 0 {
				fmt.Println(strings.Join(names, "|"))
			}
			rowstr := make([]string, len(rows.ColumnTypes()))
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
			if result.Results.RowCount == 1 {
				fmt.Printf("1 row returned\n")
			} else {
				fmt.Printf("%d rows returned\n", result.Results.RowCount)
			}

		case *service.ExecuteSQLStatementResponse_Error:
			fmt.Printf("error: %s\n", result.Error)
		}
	}
}

func toColumnTypes(result *service.ResultSet) (names []string, types []common.ColumnType) {
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
