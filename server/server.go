package server

import (
	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/command"
	"github.com/squareup/pranadb/meta"
	"github.com/squareup/pranadb/parplan"
	"github.com/squareup/pranadb/pull"
	"github.com/squareup/pranadb/push"
	"github.com/squareup/pranadb/sharder"
	"sync"
)

func NewServer(nodeID int, numShards int) *Server {
	clus := cluster.NewFakeCluster(nodeID, numShards)
	metaController := meta.NewController(clus)
	planner := parplan.NewPlanner()
	shardr := sharder.NewSharder(clus)
	pushEngine := push.NewPushEngine(clus, planner, shardr)
	clus.SetLeaderChangedCallback(pushEngine)
	clus.SetRemoteWriteHandler(pushEngine)
	pullEngine := pull.NewPullEngine(planner, clus, metaController)
	clus.SetRemoteQueryExecutionCallback(pullEngine)
	commandExecutor := command.NewCommandExecutor(metaController, pushEngine, pullEngine, clus)
	server := Server{
		nodeID:          nodeID,
		cluster:         clus,
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
	err := s.metaController.Start()
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

func (s *Server) GetSharder() *sharder.Sharder {
	return s.shardr
}

func (s *Server) GetPushEngine() *push.PushEngine {
	return s.pushEngine
}

func (s *Server) GetCluster() cluster.Cluster {
	return s.cluster
}
