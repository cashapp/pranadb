package main

import (
	"fmt"
	cli2 "github.com/squareup/pranadb/cli"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/alecthomas/kong"
	"github.com/chzyer/readline"
)

var arguments struct {
	Addr string `help:"Address of PranaDB server to connect to." default:"127.0.0.1:6584"`
	VI   bool   `help:"Enable VI mode."`
}

func main() {
	cm := &cliMain{}
	cm.run()
}

type cliMain struct {
	cl        *cli2.Cli
	kctx      *kong.Context
	sessionID string
}

func (c *cliMain) run() {
	err := c.doRun()
	if c.sessionID != "" {
		if err := c.cl.CloseSession(c.sessionID); err != nil {
			log.Printf("failed to close session %v", err)
		}
	}
	if c.cl != nil {
		if err := c.cl.Stop(); err != nil {
			log.Printf("failed to close cli %v", err)
		}
	}
	c.kctx.FatalIfErrorf(err)
}

func (c *cliMain) doRun() error {
	c.kctx = kong.Parse(&arguments)

	cl := cli2.NewCli(arguments.Addr, time.Second*5)
	if err := cl.Start(); err != nil {
		return err
	}
	c.cl = cl

	session, err := cl.CreateSession()
	if err != nil {
		return err
	}
	c.sessionID = session

	home, err := os.UserHomeDir()
	if err != nil {
		return err
	}

	rl, err := readline.NewEx(&readline.Config{
		HistoryFile:            filepath.Join(home, ".prana.history"),
		DisableAutoSaveHistory: true,
		VimMode:                arguments.VI,
	})
	if err != nil {
		return err
	}
	for {
		// Gather multi-line statement terminated by a ;
		rl.SetPrompt("pranadb> ")
		cmd := []string{}
		for {
			line, err := rl.Readline()
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return err
			}
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

		if err := c.sendStatement(statement, cl); err != nil {
			return err
		}
	}
}

func (c *cliMain) sendStatement(statement string, cli *cli2.Cli) error {
	ch, err := cli.ExecuteStatement(c.sessionID, statement)
	if err != nil {
		return err
	}
	for line := range ch {
		fmt.Println(line)
	}
	return nil
}
