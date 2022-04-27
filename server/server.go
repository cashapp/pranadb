package server

import (
	"fmt"
	"github.com/squareup/pranadb/cluster/fake"
	"github.com/squareup/pranadb/failinject"
	"net/http" //nolint:stylecheck

	"github.com/squareup/pranadb/lifecycle"
	"github.com/squareup/pranadb/metrics"

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
	"github.com/squareup/pranadb/notifier"
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
	var notifClient notifier.Client
	var notifServer notifier.Server
	if config.TestServer {
		clus = fake.NewFakeCluster(config.NodeID, config.NumShards)
		fakeNotifier := notifier.NewFakeNotifier()
		notifClient = fakeNotifier
		notifServer = fakeNotifier
	} else {
		var err error
		clus, err = dragon.NewDragon(config)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		notifServer = notifier.NewServer(config.NotifListenAddresses[config.NodeID])
		notifClient = notifier.NewClient(config.NotifierHeartbeatInterval, config.NotifListenAddresses...)
	}
	metaController := meta.NewController(clus)
	shardr := sharder.NewSharder(clus)
	pullEngine := pull.NewPullEngine(clus, metaController, shardr)
	clus.SetRemoteQueryExecutionCallback(pullEngine)
	protoRegistry := protolib.NewProtoRegistry(metaController, clus, pullEngine, config.ProtobufDescriptorDir)
	protoRegistry.SetNotifier(notifClient.BroadcastSync)
	theMetrics := metrics.NewServer(config)
	var failureInjector failinject.Injector
	if config.EnableFailureInjector {
		failureInjector = failinject.NewInjector()
	} else {
		failureInjector = failinject.NewDummyInjector()
	}
	pushEngine := push.NewPushEngine(clus, shardr, metaController, &config, pullEngine, protoRegistry, failureInjector)
	clus.RegisterShardListenerFactory(pushEngine)
	commandExecutor := command.NewCommandExecutor(metaController, pushEngine, pullEngine, clus, notifClient,
		protoRegistry, failureInjector)
	notifServer.RegisterNotificationListener(notifier.NotificationTypeDDLStatement, commandExecutor)
	notifServer.RegisterNotificationListener(notifier.NotificationTypeCloseSession, pullEngine)
	notifServer.RegisterNotificationListener(notifier.NotificationTypeReloadProtobuf, protoRegistry)
	schemaLoader := schema.NewLoader(metaController, pushEngine, pullEngine)
	clus.RegisterMembershipListener(pullEngine)
	apiServer := api.NewAPIServer(commandExecutor, protoRegistry, config)

	services := []service{
		lifeCycleMgr,
		notifServer,
		metaController,
		clus,
		shardr,
		commandExecutor,
		pushEngine,
		pullEngine,
		protoRegistry,
		schemaLoader,
		apiServer,
		theMetrics,
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
		notifServer:     notifServer,
		notifClient:     notifClient,
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
	notifServer     notifier.Server
	notifClient     notifier.Client
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

	if s.conf.Debug {
		addr := fmt.Sprintf("localhost:%d", s.cluster.GetNodeID()+6676)
		s.debugServer = &http.Server{Addr: addr}
		go func(srv *http.Server) {
			err := srv.ListenAndServe()
			if err != nil && err != http.ErrServerClosed {
				log.Errorf("debug server failed to listen %v", err)
			}
		}(s.debugServer)
	}

	var err error
	for _, s := range s.services {
		if err = s.Start(); err != nil {
			return errors.WithStack(err)
		}
	}

	if err := s.cluster.PostStartChecks(s.pullEngine); err != nil {
		return errors.WithStack(err)
	}

	if err := s.pushEngine.Ready(); err != nil {
		return errors.WithStack(err)
	}

	s.lifeCycleMgr.SetActive(true)

	s.started = true

	log.Infof("Prana server %d started", s.nodeID)

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

func (s *Server) GetNotificationsClient() notifier.Client {
	return s.notifClient
}

func (s *Server) GetNotificationsServer() notifier.Server {
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
