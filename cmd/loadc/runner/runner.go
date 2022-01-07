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

// Runner represents a command line client that runs commands from a script file against a load runner server
// The commands are expressed in the script file as a JSON array containing JSON objects - each object describes a
// command
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
	for _, command := range commands {
		com := command
		bytes, err := json.Marshal(&com)
		if err != nil {
			return err
		}
		sCommand := string(bytes)
		log.Infof("Executing command: %s", sCommand)
		_, err = client.RunCommand(context.Background(), &loadrunner.RunCommandRequest{
			CommandJson: sCommand,
		})
		if err != nil {
			return err
		}
	}
	return conn.Close()
}
