package main

import (
	"fmt"
	"github.com/squareup/pranadb/cmd/cli"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/alecthomas/kong"
	"github.com/chzyer/readline"
)

var arguments struct {
	Addr string `help:"Address of PranaDB server to connect to." default:"127.0.0.1:6584"`
	VI   bool   `help:"Enable VI mode."`
}

func main() {
	kctx := kong.Parse(&arguments)

	cl := cli.NewCli(arguments.Addr)
	err := cl.Start()
	kctx.FatalIfErrorf(err)
	defer func() {
		if err := cl.Stop(); err != nil {
			// Ignore
		}
	}()

	home, err := os.UserHomeDir()
	kctx.FatalIfErrorf(err)

	rl, err := readline.NewEx(&readline.Config{
		HistoryFile:            filepath.Join(home, ".prana.history"),
		DisableAutoSaveHistory: true,
		VimMode:                arguments.VI,
	})
	kctx.FatalIfErrorf(err)
	for {
		// Gather multi-line statement terminated by a ;
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
		statement := strings.Join(cmd, " ")
		_ = rl.SaveHistory(statement)

		err = sendStatement(statement, cl)
		if err != nil {
			kctx.Errorf("%s", err)
		}
	}
}

func sendStatement(statement string, cli *cli.Cli) error {
	ch, err := cli.ExecuteStatement(statement)
	if err != nil {
		return err
	}
	for line := range ch {
		fmt.Println(line)
	}
	return nil
}
