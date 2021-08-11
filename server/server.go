package server

import (
	"fmt"
	"github.com/squareup/pranadb/conf"
	"log"
	"net/http" //nolint:stylecheck
	"sync"

	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/cluster/dragon"
	"github.com/squareup/pranadb/command"
	"github.com/squareup/pranadb/meta"
	"github.com/squareup/pranadb/meta/schema"
	"github.com/squareup/pranadb/notifier"
	"github.com/squareup/pranadb/pull"
	"github.com/squareup/pranadb/push"
	"github.com/squareup/pranadb/sharder"

	// Disabled lint warning on the following as we're only listening on localhost so shouldn't be an issue?
	//nolint:gosec
	_ "net/http/pprof" //nolint:stylecheck
)

func NewServer(config conf.Config) (*Server, error) {
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
		notifClient = notifier.NewClient(config.NotifListenAddresses...)
	}
	metaController := meta.NewController(clus)
	shardr := sharder.NewSharder(clus)
	pullEngine := pull.NewPullEngine(clus, metaController)
	clus.SetRemoteQueryExecutionCallback(pullEngine)
	pushEngine := push.NewPushEngine(clus, shardr, metaController, &config, pullEngine)
	clus.RegisterShardListenerFactory(pushEngine)
	commandExecutor := command.NewCommandExecutor(metaController, pushEngine, pullEngine, clus, notifClient)
	notifServer.RegisterNotificationListener(notifier.NotificationTypeDDLStatement, commandExecutor)
	notifServer.RegisterNotificationListener(notifier.NotificationTypeCloseSession, pullEngine)
	schemaLoader := schema.NewLoader(metaController, pushEngine, pullEngine)
	clus.RegisterMembershipListener(pullEngine)

	services := []service{
		notifServer,
		metaController,
		clus,
		shardr,
		commandExecutor,
		pushEngine,
		pullEngine,
		schemaLoader,
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
	pushEngine      *push.PushEngine
	pullEngine      *pull.PullEngine
	commandExecutor *command.Executor
	schemaLoader    *schema.Loader
	notifServer     notifier.Server
	notifClient     notifier.Client
	services        []service
	started         bool
	conf            conf.Config
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
	for _, s := range s.services {
		if err = s.Start(); err != nil {
			return err
		}
	}
	s.started = true

	if s.conf.Debug {
		go func() {
			log.Println(http.ListenAndServe(fmt.Sprintf("localhost:%d", s.cluster.GetNodeID()+6676), nil))
		}()
	}

	return nil
}

func (s *Server) Stop() error {
	if !s.started {
		return nil
	}
	s.lock.Lock()
	defer s.lock.Unlock()
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

func (s *Server) GetPushEngine() *push.PushEngine {
	return s.pushEngine
}

func (s *Server) GetPullEngine() *pull.PullEngine {
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
