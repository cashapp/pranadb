package server

import (
	"fmt"
	"net/http" //nolint:stylecheck
	// Disabled lint warning on the following as we're only listening on localhost so shouldn't be an issue?
	//nolint:gosec
	_ "net/http/pprof" //nolint:stylecheck
	//nolint:stylecheck
	"sync"

	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/api"
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
		return nil, err
	}
	var clus cluster.Cluster
	var notifClient notifier.Client
	var notifServer notifier.Server
	if config.TestServer {
		clus = cluster.NewFakeCluster(config.NodeID, config.NumShards)
		fakeNotifier := notifier.NewFakeNotifier()
		notifClient = fakeNotifier
		notifServer = fakeNotifier
	} else {
		var err error
		clus, err = dragon.NewDragon(config)
		if err != nil {
			return nil, err
		}
		notifServer = notifier.NewServer(config.NotifListenAddresses[config.NodeID])
		notifClient = notifier.NewClient(config.NotifierHeartbeatInterval, config.NotifListenAddresses...)
	}
	metaController := meta.NewController(clus)
	shardr := sharder.NewSharder(clus)
	pullEngine := pull.NewPullEngine(clus, metaController)
	clus.SetRemoteQueryExecutionCallback(pullEngine)
	protoRegistry := protolib.NewProtoRegistry(metaController, clus, pullEngine, config.ProtobufDescriptorDir)
	protoRegistry.SetNotifier(notifClient.BroadcastSync)
	pushEngine := push.NewPushEngine(clus, shardr, metaController, &config, pullEngine, protoRegistry)
	clus.RegisterShardListenerFactory(pushEngine)
	commandExecutor := command.NewCommandExecutor(metaController, pushEngine, pullEngine, clus, notifClient)
	notifServer.RegisterNotificationListener(notifier.NotificationTypeDDLStatement, commandExecutor)
	notifServer.RegisterNotificationListener(notifier.NotificationTypeCloseSession, pullEngine)
	notifServer.RegisterNotificationListener(notifier.NotificationTypeReloadProtobuf, protoRegistry)
	schemaLoader := schema.NewLoader(metaController, pushEngine, pullEngine)
	clus.RegisterMembershipListener(pullEngine)
	apiServer := api.NewAPIServer(commandExecutor, protoRegistry, config)

	services := []service{
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
	}

	server := Server{
		conf:            config,
		nodeID:          config.NodeID,
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
	}
	return &server, nil
}

type Server struct {
	lock            sync.RWMutex
	nodeID          int
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
			if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Errorf("debug server failed to listen %v", err)
			} else {
				log.Debugf("Started debug server on address %s", addr)
			}
		}(s.debugServer)
	}

	var err error
	for _, s := range s.services {
		if err = s.Start(); err != nil {
			return err
		}
	}
	if err := s.pushEngine.Ready(); err != nil {
		return err
	}

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
	if s.debugServer != nil {
		if err := s.debugServer.Close(); err != nil {
			return err
		}
	}
	for i := len(s.services) - 1; i >= 0; i-- {
		if err := s.services[i].Stop(); err != nil {
			return err
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
