package server

import (
	"github.com/squareup/pranadb/cluster/fake"
	"github.com/squareup/pranadb/failinject"
	"github.com/squareup/pranadb/lifecycle"
	"github.com/squareup/pranadb/metrics"
	"github.com/squareup/pranadb/remoting"
	"net/http" //nolint:stylecheck
	"reflect"
	"runtime"

	// Disabled lint warning on the following as we're only listening on localhost so shouldn't be an issue?
	//nolint:gosec
	_ "net/http/pprof" //nolint:stylecheck
	//nolint:stylecheck
	"sync"

	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/api"
	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/protolib"

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
	var ddlClient remoting.Client
	var ddlResetClient remoting.Client
	var remotingServer remoting.Server
	if config.TestServer {
		clus = fake.NewFakeCluster(config.NodeID, config.NumShards)
		fakeNotifier := remoting.NewFakeServer()
		ddlClient = fakeNotifier
		remotingServer = fakeNotifier
		ddlResetClient = fakeNotifier
	} else {
		var err error
		drag, err := dragon.NewDragon(config)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		clus = drag
		remotingServer = remoting.NewServer(config.NotifListenAddresses[config.NodeID])
		ddlClient = remoting.NewClient(true, config.NotifListenAddresses...)
		ddlResetClient = remoting.NewClient(true, config.NotifListenAddresses...)
		remotingServer.RegisterMessageHandler(remoting.ClusterMessageClusterProposeRequest, drag.GetRemoteProposeHandler())
		remotingServer.RegisterMessageHandler(remoting.ClusterMessageClusterReadRequest, drag.GetRemoteReadHandler())
	}
	metaController := meta.NewController(clus)
	shardr := sharder.NewSharder(clus)
	pullEngine := pull.NewPullEngine(clus, metaController, shardr)
	clus.SetRemoteQueryExecutionCallback(pullEngine)
	protoRegistry := protolib.NewProtoRegistry(metaController, clus, pullEngine, config.ProtobufDescriptorDir)
	protoRegistry.SetNotifier(ddlClient.BroadcastSync)
	theMetrics := metrics.NewServer(config, !config.EnableMetrics)
	var failureInjector failinject.Injector
	if config.EnableFailureInjector {
		failureInjector = failinject.NewInjector()
	} else {
		failureInjector = failinject.NewDummyInjector()
	}
	pushEngine := push.NewPushEngine(clus, shardr, metaController, &config, pullEngine, protoRegistry, failureInjector)
	clus.RegisterShardListenerFactory(pushEngine)
	remotingServer.RegisterMessageHandler(remoting.ClusterMessageConsumerSetRate, pushEngine.GetLoadClientSetRateHandler())
	remotingServer.RegisterMessageHandler(remoting.ClusterMessageForwardWriteRequest, pushEngine.GetForwardWriteHandler())
	commandExecutor := command.NewCommandExecutor(metaController, pushEngine, pullEngine, clus, ddlClient, ddlResetClient,
		protoRegistry, failureInjector, &config)
	remotingServer.RegisterMessageHandler(remoting.ClusterMessageDDLStatement, commandExecutor.DDlCommandRunner().DdlHandler())
	remotingServer.RegisterMessageHandler(remoting.ClusterMessageDDLCancel, commandExecutor.DDlCommandRunner().CancelHandler())
	remotingServer.RegisterMessageHandler(remoting.ClusterMessageReloadProtobuf, protoRegistry)
	schemaLoader := schema.NewLoader(metaController, pushEngine, pullEngine)
	apiServer := api.NewAPIServer(metaController, commandExecutor, protoRegistry, config)

	services := []service{
		lifeCycleMgr,
		metaController,
		remotingServer,
		clus,
		shardr,
		commandExecutor,
		pushEngine,
		pullEngine,
		protoRegistry,
		schemaLoader,
		theMetrics,
		apiServer,
		failureInjector,
	}

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
		notifServer:     remotingServer,
		ddlClient:       ddlClient,
		apiServer:       apiServer,
		services:        services,
		metrics:         theMetrics,
		failureinjector: failureInjector,
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
	notifServer     remoting.Server
	ddlClient       remoting.Client
	apiServer       *api.Server
	services        []service
	started         bool
	conf            conf.Config
	debugServer     *http.Server
	metrics         *metrics.Server
	failureinjector failinject.Injector
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

	if err := s.pushEngine.Ready(); err != nil {
		return errors.WithStack(err)
	}

	s.pullEngine.SetAvailable()

	s.lifeCycleMgr.SetActive(true)

	s.started = true

	log.Infof("Prana server %d started on %s with %d CPUs", s.nodeID, runtime.GOOS, runtime.NumCPU())
	return nil
}

func (s *Server) Stop() error {
	if !s.started {
		return nil
	}
	s.lock.Lock()
	defer s.lock.Unlock()
	s.lifeCycleMgr.SetActive(false)
	if s.debugServer != nil {
		if err := s.debugServer.Close(); err != nil {
			return errors.WithStack(err)
		}
	}
	for i := len(s.services) - 1; i >= 0; i-- {
		log.Infof("prana node %d stopping service %s", s.nodeID, reflect.TypeOf(s.services[i]).String())
		if err := s.services[i].Stop(); err != nil {
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

func (s *Server) GetNotificationsClient() remoting.Client {
	return s.ddlClient
}

func (s *Server) GetNotificationsServer() remoting.Server {
	return s.notifServer
}

func (s *Server) GetAPIServer() *api.Server {
	return s.apiServer
}

func (s *Server) GetConfig() conf.Config {
	return s.conf
}

func (s *Server) GetFailureInjector() failinject.Injector {
	return s.failureinjector
}
