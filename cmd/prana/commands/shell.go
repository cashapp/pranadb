package commands

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/chzyer/readline"
	"github.com/squareup/pranadb/client"
	"github.com/squareup/pranadb/errors"
)

type ShellCommand struct {
	VI bool `help:"Enable VI mode."`
}

func (c *ShellCommand) Run(cl *client.Client) error {
	home, err := os.UserHomeDir()
	if err != nil {
		return errors.WithStack(err)
	}

	rl, err := readline.NewEx(&readline.Config{
		HistoryFile:            filepath.Join(home, ".prana.history"),
		DisableAutoSaveHistory: true,
		VimMode:                c.VI,
	})
	if err != nil {
		return errors.WithStack(err)
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
				if err.Error() == "Interrupt" {
					// This occurs when CTRL-C is pressed - we should exit silently
					return nil
				}
				return errors.WithStack(err)
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
			return errors.WithStack(err)
		}
	}
}

func (c *ShellCommand) sendStatement(statement string, cli *client.Client) error {
	ch, err := cli.ExecuteStatement(statement, nil, nil)
	if err != nil {
		return errors.WithStack(err)
	}
	for line := range ch {
		fmt.Println(line)
	}
	return nil
}
