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

	"github.com/squareup/pranadb/protos/squareup/cash/pranadb/v1/service"
)

var cli struct {
	Addr string `help:"Address of PranaDB server to connect to." default:"127.0.0.1:6584"`
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
		case *service.ExecuteSQLStatementResponse_Error:
			fmt.Printf("error: %s\n", result.Error)
		}
	}
}
