package loadrunner

import (
	"context"
	"encoding/json"
	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/protos/squareup/cash/pranadb/v1/loadrunner"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/types/known/emptypb"
	"net"
	"sync"
)

// LoadRunner is a simple server which hosts a gRPC API and allows commands to be executed. Commands are described by
// JSON and can be things like sending messages to Kafka or executing Prana DML or DDL statements against a Prana server.
// Load runner is deployed as a cluster and there's a simple command line client that's used to send commands to it.
type LoadRunner struct {
	lock             sync.RWMutex
	started          bool
	conf             Config
	gsrv             *grpc.Server
	commandFactories map[string]CommandFactory
}

const (
	commandNameField      = "command_name"
	loadRunnerNodeIDField = "load_runner_node_id"
)

func NewLoadRunner(conf Config) *LoadRunner {
	return &LoadRunner{commandFactories: make(map[string]CommandFactory), conf: conf}
}

func (p *LoadRunner) Start() error {
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.started {
		return nil
	}
	address := p.conf.APIServerListenAddresses[p.conf.NodeID]
	list, err := net.Listen("tcp", address)
	if err != nil {
		return errors.WithStack(err)
	}
	p.gsrv = grpc.NewServer()
	reflection.Register(p.gsrv)
	loadrunner.RegisterLoadRunnerServiceServer(p.gsrv, p)
	if err := p.registerCommands(); err != nil {
		log.Printf("Failed to register commands %v", err)
		return err
	}
	p.started = true
	go p.startServer(list)
	log.Infof("Started LoadRunner with ID %d on address %s", p.conf.NodeID, address)
	return nil
}

func (p *LoadRunner) Stop() error {
	p.lock.Lock()
	defer p.lock.Unlock()
	if !p.started {
		return nil
	}
	p.gsrv.Stop()
	return nil
}

func (p *LoadRunner) startServer(list net.Listener) {
	err := p.gsrv.Serve(list) //nolint:ifshort
	p.lock.Lock()
	defer p.lock.Unlock()
	p.started = false
	if err != nil {
		log.Errorf("loadrunner grpc server listen failed: %v", err)
	}
}

func (p *LoadRunner) RunCommand(ctx context.Context, request *loadrunner.RunCommandRequest) (*emptypb.Empty, error) {

	log.Infof("Load-runner received command %s", request.CommandJson)

	p.lock.RLock()
	defer p.lock.RUnlock()
	jsonString := request.CommandJson

	commandConfig := make(map[string]interface{})
	if err := json.Unmarshal([]byte(jsonString), &commandConfig); err != nil {
		return nil, err
	}
	oCommandName, ok := commandConfig[commandNameField]
	if !ok {
		return nil, errors.Errorf("missing %s field in commandConfig %s", commandNameField, jsonString)
	}
	sCommandName, ok := oCommandName.(string)
	if !ok {
		return nil, errors.Errorf("Invalid %s field %v", commandNameField, oCommandName)
	}
	oLoadRunnerNodeID, ok := commandConfig[loadRunnerNodeIDField]
	if !ok {
		return nil, errors.Errorf("Missing %s field in commandConfig", loadRunnerNodeIDField)
	}
	fLoadRunnerNodeID, ok := oLoadRunnerNodeID.(float64)
	if !ok {
		return nil, errors.Errorf("Invalid %s field %v", loadRunnerNodeIDField, oLoadRunnerNodeID)
	}
	loadRunnerNodeID := int(fLoadRunnerNodeID)

	if loadRunnerNodeID != p.conf.NodeID {
		err := p.forwardCommand(loadRunnerNodeID, jsonString)
		log.Errorf("Forward commandConfig returned with err %v", err)
		return &emptypb.Empty{}, err
	}
	log.Infof("Executing command on node %d command name is %s", loadRunnerNodeID, sCommandName)
	commandFactory, ok := p.commandFactories[sCommandName]
	if !ok {
		return nil, errors.NewUnknownLoadRunnerfCommandError(request.CommandJson)
	}
	command := commandFactory.CreateCommand(loadRunnerNodeID, jsonString)
	err := command.Run()
	if err != nil {
		log.Printf("failed to run commandConfig %v", err)
	}
	return &emptypb.Empty{}, err
}

func (p *LoadRunner) forwardCommand(loadRunnerNodeID int, command string) error {
	log.Infof("Forwarding command to node %d", loadRunnerNodeID)
	if loadRunnerNodeID < 0 || loadRunnerNodeID >= len(p.conf.APIServerListenAddresses) {
		return errors.Errorf("invalid load runner node id %d", loadRunnerNodeID)
	}
	address := p.conf.APIServerListenAddresses[loadRunnerNodeID]
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		return errors.WithStack(err)
	}
	client := loadrunner.NewLoadRunnerServiceClient(conn)
	_, err = client.RunCommand(context.Background(), &loadrunner.RunCommandRequest{
		CommandJson: command,
	})
	if err := conn.Close(); err != nil {
		return err
	}
	return err
}

func (p *LoadRunner) registerCommandFactory(factory CommandFactory) error {
	_, ok := p.commandFactories[factory.Name()]
	if ok {
		return errors.Errorf("command factory already registered with name %s", factory.Name())
	}
	p.commandFactories[factory.Name()] = factory
	return nil
}

func (p *LoadRunner) registerCommands() error {
	if err := p.registerCommandFactory(&PublishCommandFactory{}); err != nil {
		return err
	}
	return p.registerCommandFactory(&ExecStatementCommandFactory{})
}
