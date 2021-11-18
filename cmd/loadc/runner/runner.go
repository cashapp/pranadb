package runner

import (
	"context"
	"encoding/json"
	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/protos/squareup/cash/pranadb/v1/loadrunner"
	"google.golang.org/grpc"
	"io/ioutil"
)

type Runner struct {
	scriptFile    string
	serverAddress string
}

func NewRunner(scriptFile string, serverAddress string) *Runner {
	return &Runner{
		scriptFile:    scriptFile,
		serverAddress: serverAddress,
	}
}

func (r *Runner) Run() error {
	conn, err := grpc.Dial(r.serverAddress, grpc.WithInsecure())
	if err != nil {
		return errors.WithStack(err)
	}
	client := loadrunner.NewLoadRunnerServiceClient(conn)
	scriptData, err := ioutil.ReadFile(r.scriptFile)
	if err != nil {
		return err
	}
	var commands []map[string]interface{}
	if err := json.Unmarshal(scriptData, &commands); err != nil {
		return err
	}
	log.Printf("commands %v", commands)
	for _, command := range commands {
		log.Printf("Got command %v", command)
		// Kind of clunky
		com := command
		bytes, err := json.Marshal(&com)
		if err != nil {
			return err
		}
		_, err = client.RunCommand(context.Background(), &loadrunner.RunCommandRequest{
			CommandJson: string(bytes),
		})
		if err != nil {
			return err
		}
	}
	return conn.Close()
}
