package server

import (
	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/command"
	"github.com/squareup/pranadb/meta"
	"github.com/squareup/pranadb/parplan"
	"github.com/squareup/pranadb/pull"
	"github.com/squareup/pranadb/push"
	"github.com/squareup/pranadb/storage"
	"sync"
)

func NewServer(nodeID int) *Server {
	clusterMgr := cluster.NewFakeClusterManager(nodeID, 10)
	store := storage.NewFakeStorage()
	metaController := meta.NewController()
	planner := parplan.NewPlanner()
	pushEngine := push.NewPushEngine(store, clusterMgr, planner)
	pullEngine := pull.NewPullEngine(planner)
	commandExecutor := command.NewCommandExecutor(store, metaController, pushEngine, pullEngine, clusterMgr)
	server := Server{
		nodeID:          nodeID,
		storage:         store,
		clusterMgr:      clusterMgr,
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
	clusterMgr      cluster.Cluster
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
	err = s.clusterMgr.Start()
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
	err := s.clusterMgr.Stop()
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
