package loadrunner

import (
	"encoding/json"
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/client"
)

// ExecStatementCommandFactory is a CommandFactory instance that creates ExecuteStatementCommand instances
type ExecStatementCommandFactory struct {
}

func (p *ExecStatementCommandFactory) CreateCommand(loadRunnerNodeID int, commandString string) Command {
	return &ExecStatementCommand{loadRunnerNodeID: loadRunnerNodeID, commandString: commandString}
}

func (p *ExecStatementCommandFactory) Name() string {
	return "exec_stmt"
}

// ExecStatementCommand is a Command that executes Prana statements (e.g. SELECT statements, or DDL statements like CREATE SOURCE)
// against Prana
type ExecStatementCommand struct {
	loadRunnerNodeID int
	commandString    string
}

type ExecuteStatementCommandCfg struct {
	// LoadRunnerNodeID is the ID of the load runner node from where to execute this statement
	LoadRunnerNodeID int `json:"load_runner_node_id"`
	// NumIters is the number of times to execute the statement
	NumIters int `json:"num_iters"`
	// Concurrency determines the number of goroutines that will be created to execute the statements. Each
	// goroutine executes the statement numIters times
	Concurrency int `json:"concurrency"`
	// The gRPC API host_name of the Prana server to execute the statement on
	PranaHostname string `json:"prana_host_name"`
	// SchemaName is the schema name
	SchemaName string `json:"schema_name"`
	// Statement is the statement to execute
	Statement string `json:"statement"`
}

func (p *ExecStatementCommand) Run() error {
	cfg := &ExecuteStatementCommandCfg{}
	if err := json.Unmarshal([]byte(p.commandString), cfg); err != nil {
		return err
	}
	log.Infof("Running execute statement: %s with concurrency %d", p.commandString, cfg.Concurrency)
	chans := make([]chan error, cfg.Concurrency)
	for i := 0; i < cfg.Concurrency; i++ {
		ch := make(chan error)
		chans[i] = ch
		go p.runStatementsWithCh(ch, cfg.NumIters, cfg.PranaHostname, cfg.SchemaName, cfg.Statement)
	}
	for _, ch := range chans {
		err := <-ch
		if err != nil {
			log.Errorf("failed to execute statement %v", err)
			return err
		}
	}
	return nil
}

func (p *ExecStatementCommand) runStatementsWithCh(ch chan error, numIters int, pranaHostname string, schemaName string, statement string) {
	ch <- p.runStatements(numIters, pranaHostname, schemaName, statement)
}

func (p *ExecStatementCommand) runStatements(numIters int, pranaHostname string, schemaName string, statement string) error {
	pranaClient := client.NewClient(pranaHostname)
	if err := pranaClient.Start(); err != nil {
		return err
	}
	resultCh, err := pranaClient.ExecuteStatement(fmt.Sprintf("use %s", schemaName), nil)
	if err != nil {
		return err
	}
	for line := range resultCh {
		log.Println(line)
	}
	for i := 0; i < numIters; i++ {
		ch, err := pranaClient.ExecuteStatement(statement, nil)
		if err != nil {
			return err
		}
		// consume the results
		for line := range ch {
			log.Println(line)
		}
	}
	return pranaClient.Stop()
}
