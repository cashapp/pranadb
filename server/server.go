package server

import (
	"fmt"
	"github.com/squareup/pranadb/api/grpc"
	"github.com/squareup/pranadb/cluster/fake"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/failinject"
	"github.com/squareup/pranadb/lifecycle"
	"github.com/squareup/pranadb/metrics"
	"github.com/squareup/pranadb/remoting"
	"gopkg.in/DataDog/dd-trace-go.v1/profiler"
	"net/http" //nolint:stylecheck
	"os"
	"reflect"
	"runtime"
	"strings"

	// Disabled lint warning on the following as we're only listening on localhost so shouldn't be an issue?
	//nolint:gosec
	_ "net/http/pprof" //nolint:stylecheck
	//nolint:stylecheck
	"sync"

	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/protolib"

	phttp "github.com/squareup/pranadb/api/http"
	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/cluster/dragon"
	"github.com/squareup/pranadb/command"
	"github.com/squareup/pranadb/conf"
	"github.com/squareup/pranadb/meta"
	"github.com/squareup/pranadb/meta/schema"
	"github.com/squareup/pranadb/pull"
	"github.com/squareup/pranadb/push"
	"github.com/squareup/pranadb/sharder"
)

func NewServer(config conf.Config) (*Server, error) {
	if err := config.Validate(); err != nil {
		return nil, errors.WithStack(err)
	}
	lifeCycleMgr := lifecycle.NewLifecycleEndpoints(config)
	var clus cluster.Cluster
	var ddlClient remoting.Broadcaster
	var ddlResetClient remoting.Broadcaster
	var remotingServer remoting.Server
	stopSignaller := &common.AtomicBool{}
	var drag *dragon.Dragon
	if config.TestServer {
		clus = fake.NewFakeCluster(config.NodeID, config.NumShards)
		fakeRemotingServer := remoting.NewFakeServer()
		remotingServer = fakeRemotingServer
		ddlClient = remoting.NewFakeClient(fakeRemotingServer)
		ddlResetClient = remoting.NewFakeClient(fakeRemotingServer)
	} else {
		var err error
		drag, err = dragon.NewDragon(config, stopSignaller)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		clus = drag
		remotingServer = remoting.NewServer(config.NotifListenAddresses[config.NodeID])
		ddlClient = remoting.NewBroadcastWrapper(config.NotifListenAddresses...)
		ddlResetClient = remoting.NewBroadcastWrapper(config.NotifListenAddresses...)
		remotingServer.RegisterMessageHandler(remoting.ClusterMessageClusterProposeRequest, drag.GetRemoteProposeHandler())
		remotingServer.RegisterMessageHandler(remoting.ClusterMessageClusterReadRequest, drag.GetRemoteReadHandler())
		remotingServer.RegisterMessageHandler(remoting.ClusterMessageLeaderInfos, drag.GetLeaderInfosHandler())
	}
	metaController := meta.NewController(clus)
	shardr := sharder.NewSharder(clus)
	pullEngine := pull.NewPullEngine(clus, metaController, shardr)
	clus.SetRemoteQueryExecutionCallback(pullEngine)
	protoRegistry := protolib.NewProtoRegistry(metaController, clus, pullEngine, config.ProtobufDescriptorDir)
	protoRegistry.SetNotifier(ddlClient)
	theMetrics := metrics.NewServer(config, !config.EnableMetrics)
	var failureInjector failinject.Injector
	if config.EnableFailureInjector {
		failureInjector = failinject.NewInjector()
	} else {
		failureInjector = failinject.NewDummyInjector()
	}
	pushEngine := push.NewPushEngine(clus, shardr, metaController, &config, pullEngine, protoRegistry, failureInjector)
	clus.RegisterShardListenerFactory(pushEngine)
	if drag != nil {
		drag.SetForwardWriteHandler(pushEngine)
	}
	remotingServer.RegisterMessageHandler(remoting.ClusterMessageSourceSetMaxRate, pushEngine.GetLoadClientSetRateHandler())
	remotingServer.RegisterMessageHandler(remoting.ClusterMessageForwardWriteRequest, pushEngine.GetForwardWriteHandler())
	commandExecutor := command.NewCommandExecutor(metaController, pushEngine, pullEngine, clus, ddlClient, ddlResetClient,
		protoRegistry, failureInjector, &config)
	remotingServer.RegisterMessageHandler(remoting.ClusterMessageDDLStatement, commandExecutor.DDlCommandRunner().DdlHandler())
	remotingServer.RegisterMessageHandler(remoting.ClusterMessageDDLCancel, commandExecutor.DDlCommandRunner().CancelHandler())
	remotingServer.RegisterMessageHandler(remoting.ClusterMessageReloadProtobuf, protoRegistry)
	schemaLoader := schema.NewLoader(metaController, pushEngine, pullEngine)
	var grpcServer *grpc.GRPCAPIServer
	if config.EnableGRPCAPIServer {
		grpcServer = grpc.NewGRPCAPIServer(metaController, commandExecutor, protoRegistry, config)
	}
	var httpAPIServer *phttp.HTTPAPIServer
	if config.EnableHTTPAPIServer {
		listenAddress := config.HTTPAPIServerListenAddresses[config.NodeID]
		httpAPIServer = phttp.NewHTTPAPIServer(listenAddress, "/pranadb", commandExecutor, metaController,
			protoRegistry, config.HTTPAPIServerTLSConfig)
	}

	services := []service{
		lifeCycleMgr,
		metaController,
		remotingServer,
		clus,
		shardr,
		commandExecutor,
		pullEngine,
		protoRegistry,
		schemaLoader,
		pushEngine,
		theMetrics,
	}
	if grpcServer != nil {
		services = append(services, grpcServer)
	}
	if httpAPIServer != nil {
		services = append(services, httpAPIServer)
	}
	services = append(services, failureInjector)

	server := Server{
		conf:            config,
		nodeID:          config.NodeID,
		lifeCycleMgr:    lifeCycleMgr,
		cluster:         clus,
		shardr:          shardr,
		metaController:  metaController,
		pushEngine:      pushEngine,
		pullEngine:      pullEngine,
		commandExecutor: commandExecutor,
		schemaLoader:    schemaLoader,
		remotingServer:  remotingServer,
		ddlClient:       ddlClient,
		grpcServer:      grpcServer,
		services:        services,
		metrics:         theMetrics,
		failureinjector: failureInjector,
		stopSignaller:   stopSignaller,
	}
	return &server, nil
}

type Server struct {
	lock            sync.RWMutex
	nodeID          int
	lifeCycleMgr    *lifecycle.Endpoints
	cluster         cluster.Cluster
	shardr          *sharder.Sharder
	metaController  *meta.Controller
	pushEngine      *push.Engine
	pullEngine      *pull.Engine
	commandExecutor *command.Executor
	schemaLoader    *schema.Loader
	remotingServer  remoting.Server
	ddlClient       remoting.Broadcaster
	grpcServer      *grpc.GRPCAPIServer
	services        []service
	started         bool
	conf            conf.Config
	debugServer     *http.Server
	metrics         *metrics.Server
	failureinjector failinject.Injector
	stopSignaller   *common.AtomicBool
}

type service interface {
	Start() error
	Stop() error
}

func (s *Server) Start() error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.started {
		return nil
	}
	s.stopSignaller.Set(false)

	if err := s.maybeEnabledDatadogProfiler(); err != nil {
		return err
	}

	var err error
	for _, serv := range s.services {
		log.Printf("prana node %d starting service %s", s.nodeID, reflect.TypeOf(serv).String())
		if err = serv.Start(); err != nil {
			return errors.WithStack(err)
		}
	}

	if err := s.cluster.PostStartChecks(s.pullEngine); err != nil {
		return errors.WithStack(err)
	}

	s.pushEngine.StartSources()

	if err := s.pullEngine.SetAvailable(); err != nil {
		return err
	}

	s.lifeCycleMgr.SetActive(true)

	s.started = true

	log.Infof("Prana server %d started on %s with %d CPUs", s.nodeID, runtime.GOOS, runtime.NumCPU())
	return nil
}

func (s *Server) maybeEnabledDatadogProfiler() error {
	ddProfileTypes := s.conf.DDProfilerTypes
	if ddProfileTypes == "" {
		return nil
	}

	ddHost := os.Getenv(s.conf.DDProfilerHostEnvVarName)
	if ddHost == "" {
		return errors.NewPranaErrorf(errors.InvalidConfiguration, "Env var %s for DD profiler host is not set", s.conf.DDProfilerHostEnvVarName)
	}

	var profileTypes []profiler.ProfileType
	aProfTypes := strings.Split(ddProfileTypes, ",")
	for _, sProfType := range aProfTypes {
		switch sProfType {
		case "CPU":
			profileTypes = append(profileTypes, profiler.CPUProfile)
		case "HEAP":
			profileTypes = append(profileTypes, profiler.HeapProfile)
		case "BLOCK":
			profileTypes = append(profileTypes, profiler.BlockProfile)
		case "MUTEX":
			profileTypes = append(profileTypes, profiler.MutexProfile)
		case "GOROUTINE":
			profileTypes = append(profileTypes, profiler.GoroutineProfile)
		default:
			return errors.NewPranaErrorf(errors.InvalidConfiguration, "Unknown Datadog profile type: %s", sProfType)
		}
	}

	agentAddress := fmt.Sprintf("%s:%d", ddHost, s.conf.DDProfilerPort)

	log.Debugf("starting Datadog continuous profiler with service name: %s environment %s version %s agent address %s profile types %s",
		s.conf.DDProfilerServiceName, s.conf.DDProfilerEnvironmentName, s.conf.DDProfilerVersionName, agentAddress, ddProfileTypes)

	return profiler.Start(
		profiler.WithService(s.conf.DDProfilerServiceName),
		profiler.WithEnv(s.conf.DDProfilerEnvironmentName),
		profiler.WithVersion(s.conf.DDProfilerVersionName),
		profiler.WithAgentAddr(agentAddress),
		profiler.WithProfileTypes(profileTypes...),
	)
}

func (s *Server) Stop() error {
	if !s.started {
		return nil
	}
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.conf.DDProfilerTypes != "" {
		profiler.Stop()
	}
	s.stopSignaller.Set(true)
	s.lifeCycleMgr.SetActive(false)
	if s.debugServer != nil {
		if err := s.debugServer.Close(); err != nil {
			return errors.WithStack(err)
		}
	}
	for i := len(s.services) - 1; i >= 0; i-- {
		serv := s.services[i]
		log.Infof("prana node %d stopping service %s", s.nodeID, reflect.TypeOf(serv).String())
		if err := serv.Stop(); err != nil {
			return errors.WithStack(err)
		}
	}
	s.started = false
	return nil
}

func (s *Server) GetMetaController() *meta.Controller {
	return s.metaController
}

func (s *Server) GetSharder() *sharder.Sharder {
	return s.shardr
}

func (s *Server) GetPushEngine() *push.Engine {
	return s.pushEngine
}

func (s *Server) GetPullEngine() *pull.Engine {
	return s.pullEngine
}

func (s *Server) GetCluster() cluster.Cluster {
	return s.cluster
}

func (s *Server) GetCommandExecutor() *command.Executor {
	return s.commandExecutor
}

func (s *Server) GetDDLClient() remoting.Broadcaster {
	return s.ddlClient
}

func (s *Server) GetRemotingServer() remoting.Server {
	return s.remotingServer
}

func (s *Server) GetGRPCServer() *grpc.GRPCAPIServer {
	return s.grpcServer
}

func (s *Server) GetConfig() conf.Config {
	return s.conf
}

func (s *Server) GetFailureInjector() failinject.Injector {
	return s.failureinjector
}
