package commands

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/chzyer/readline"
	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/client"
)

type ShellCommand struct {
	VI bool `help:"Enable VI mode."`
}

func (c *ShellCommand) Run(cl *client.Client) error {
	sessionID, err := cl.CreateSession()
	if err != nil {
		return err
	}
	defer func() {
		if err := cl.CloseSession(sessionID); err != nil {
			log.Errorf("failed to close session %+v", err)
		}
	}()

	home, err := os.UserHomeDir()
	if err != nil {
		return err
	}

	rl, err := readline.NewEx(&readline.Config{
		HistoryFile:            filepath.Join(home, ".prana.history"),
		DisableAutoSaveHistory: true,
		VimMode:                c.VI,
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

		if err := c.sendStatement(sessionID, statement, cl); err != nil {
			return err
		}
	}
}

func (c *ShellCommand) sendStatement(sessionID string, statement string, cli *client.Client) error {
	ch, err := cli.ExecuteStatement(sessionID, statement)
	if err != nil {
		return err
	}
	for line := range ch {
		fmt.Println(line)
	}
	return nil
}
