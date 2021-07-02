package server

import (
	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/command"
	"github.com/squareup/pranadb/meta"
	"github.com/squareup/pranadb/parplan"
	"github.com/squareup/pranadb/pull"
	"github.com/squareup/pranadb/push"
	"github.com/squareup/pranadb/sharder"
	"github.com/squareup/pranadb/storage"
	"sync"
)

func NewServer(nodeID int) *Server {
	cluster := cluster.NewFakeClusterManager(nodeID, 10)
	store := storage.NewFakeStorage()
	metaController := meta.NewController(store)
	planner := parplan.NewPlanner()
	shardr := sharder.NewSharder(cluster)
	pushEngine := push.NewPushEngine(store, cluster, planner, shardr)
	pullEngine := pull.NewPullEngine(planner, store, cluster, metaController)
	commandExecutor := command.NewCommandExecutor(store, metaController, pushEngine, pullEngine, cluster)
	server := Server{
		nodeID:          nodeID,
		storage:         store,
		cluster:         cluster,
		shardr:          shardr,
		metaController:  metaController,
		pushEngine:      pushEngine,
		pullEngine:      pullEngine,
		commandExecutor: commandExecutor,
	}
	return &server
}

type Server struct {
	lock            sync.RWMutex
	nodeID          int
	storage         storage.Storage
	cluster         cluster.Cluster
	shardr          *sharder.Sharder
	metaController  *meta.Controller
	pushEngine      *push.PushEngine
	pullEngine      *pull.PullEngine
	commandExecutor *command.Executor
	started         bool
}

func (s *Server) Start() error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.started {
		return nil
	}
	err := s.storage.Start()
	if err != nil {
		return err
	}
	err = s.metaController.Start()
	if err != nil {
		return err
	}
	err = s.cluster.Start()
	if err != nil {
		return err
	}
	err = s.shardr.Start()
	if err != nil {
		return err
	}
	err = s.pushEngine.Start()
	if err != nil {
		return err
	}
	err = s.pullEngine.Start()
	if err != nil {
		return err
	}
	s.started = true
	return nil
}

func (s *Server) Stop() error {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.pushEngine.Stop()
	s.pullEngine.Stop()
	err := s.shardr.Stop()
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
	err = s.storage.Stop()
	if err != nil {
		return err
	}
	if !s.started {
		return nil
	}
	s.started = false
	return nil
}

// GetCommandExecutor is for testing
func (s *Server) GetCommandExecutor() *command.Executor {
	return s.commandExecutor
}

func (s *Server) GetMetaController() *meta.Controller {
	return s.metaController
}

func (s *Server) GetStorage() storage.Storage {
	return s.storage
}
