package server

import (
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
)

type Config struct {
	NodeID               int
	ClusterID            int // All nodes in a Prana cluster must share the same ClusterID
	RaftAddresses        []string
	NotifListenAddresses []string
	NumShards            int
	ReplicationFactor    int
	DataDir              string
	TestServer           bool
}

func NewServer(config Config) (*Server, error) {
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
		clus, err = dragon.NewDragon(config.NodeID, config.ClusterID, config.RaftAddresses, config.NumShards, config.DataDir, config.ReplicationFactor, false)
		if err != nil {
			return nil, err
		}
		notifServer = notifier.NewServer(config.NotifListenAddresses[config.NodeID])
		notifClient = notifier.NewClient(config.NotifListenAddresses...)
	}

	metaController := meta.NewController(clus)
	shardr := sharder.NewSharder(clus)
	pushEngine := push.NewPushEngine(clus, shardr, metaController)
	clus.RegisterShardListenerFactory(pushEngine)
	pullEngine := pull.NewPullEngine(clus, metaController)

	clus.SetRemoteQueryExecutionCallback(pullEngine)

	commandExecutor := command.NewCommandExecutor(metaController, pushEngine, pullEngine, clus, notifClient)
	notifServer.RegisterNotificationListener(notifier.NotificationTypeDDLStatement, commandExecutor)
	notifServer.RegisterNotificationListener(notifier.NotificationTypeCloseSession, pullEngine)
	schemaLoader := schema.NewLoader(metaController, commandExecutor, pushEngine)

	clus.RegisterMembershipListener(pullEngine)
	server := Server{
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
	started         bool
}

func (s *Server) Start() error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.started {
		return nil
	}

	type service interface {
		Start() error
	}
	services := []service{
		s.notifServer,
		s.metaController,
		s.cluster,
		s.shardr,
		s.pushEngine,
		s.pullEngine,
		s.schemaLoader,
	}
	var err error
	for _, s := range services {
		if err = s.Start(); err != nil {
			return err
		}
	}
	s.started = true
	return nil
}

func (s *Server) Stop() error {
	if !s.started {
		return nil
	}
	s.lock.Lock()
	defer s.lock.Unlock()
	err := s.notifServer.Stop()
	if err != nil {
		return err
	}
	err = s.pushEngine.Stop()
	if err != nil {
		return err
	}
	s.pullEngine.Stop()
	err = s.shardr.Stop()
	if err != nil {
		return err
	}
	err = s.cluster.Stop()
	if err != nil {
		return err
	}
	err = s.metaController.Stop()
	if err != nil {
		return err
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
