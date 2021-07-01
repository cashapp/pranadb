package pull

import (
	"fmt"
	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/parplan"
	"github.com/squareup/pranadb/pull/exec"
	"github.com/squareup/pranadb/storage"
	"sync"
	"sync/atomic"
)

type PullEngine struct {
	lock            sync.RWMutex
	started         bool
	planner         *parplan.Planner
	storage         storage.Storage
	remoteQueries   map[string]exec.PullExecutor
	cluster         cluster.Cluster
	nodeID          int
	queryIDSequence int64
}

func NewPullEngine(planner *parplan.Planner, storage storage.Storage, cluster cluster.Cluster) *PullEngine {
	engine := PullEngine{
		planner:       planner,
		storage:       storage,
		remoteQueries: make(map[string]exec.PullExecutor),
		cluster:       cluster,
		nodeID:        cluster.GetNodeID(),
	}
	return &engine
}

func (p *PullEngine) Start() error {
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.started {
		return nil
	}
	p.cluster.SetRemoteQueryExecutionCallback(p)
	p.started = true
	return nil
}

func (p *PullEngine) Stop() {
	p.lock.Lock()
	defer p.lock.Unlock()
	if !p.started {
		return
	}
	p.cluster.SetRemoteQueryExecutionCallback(nil)
	p.started = false
}

func (p *PullEngine) ExecutePullQuery(schema *common.Schema, query string) (queryDAG exec.PullExecutor, err error) {
	seq := atomic.AddInt64(&p.queryIDSequence, 1)
	queryID := fmt.Sprintf("%d-%d", p.nodeID, seq)
	return p.buildPullQueryExecution(schema, query, queryID)
}

// TODO one-shot optimisation - no need to register query
func (p *PullEngine) ExecuteRemotePullQuery(serializedDag []byte, queryID string, limit int) (*common.Rows, error) {
	dag, err := p.getDag(serializedDag, queryID)
	if err != nil {
		return nil, err
	}
	rows, err := dag.GetRows(limit)
	if err != nil {
		return nil, err
	}
	if rows.RowCount() == 0 {
		// TODO query timeouts
		p.deleteQuery(queryID)
	}
	return rows, err
}

func (p *PullEngine) deleteQuery(queryID string) {
	p.lock.RLock()
	defer p.lock.RUnlock()

	delete(p.remoteQueries, queryID)
}

func (p *PullEngine) getDag(serializedDag []byte, queryID string) (exec.PullExecutor, error) {
	p.lock.RLock()
	defer p.lock.RUnlock()

	if serializedDag != nil {
		dag, _, err := exec.DeserializeDAG(serializedDag, 0)
		if err != nil {
			return nil, err
		}
		p.remoteQueries[queryID] = dag
		return dag, nil
	} else {
		dag, ok := p.remoteQueries[queryID]
		if !ok {
			return nil, fmt.Errorf("no such query %s", queryID)
		}
		return dag, nil
	}
}
