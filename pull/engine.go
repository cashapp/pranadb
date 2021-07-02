package pull

import (
	"errors"
	"fmt"
	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/meta"
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
	metaController  *meta.Controller
	nodeID          int
	queryIDSequence int64
}

func NewPullEngine(planner *parplan.Planner, storage storage.Storage, cluster cluster.Cluster, metaController *meta.Controller) *PullEngine {
	engine := PullEngine{
		planner:        planner,
		storage:        storage,
		remoteQueries:  make(map[string]exec.PullExecutor),
		cluster:        cluster,
		metaController: metaController,
		nodeID:         cluster.GetNodeID(),
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
func (p *PullEngine) ExecuteRemotePullQuery(schemaName string, query string, queryID string, limit int) (*common.Rows, error) {

	p.lock.RLock()
	defer p.lock.RUnlock()

	// TODO prepared statements - we will need this for efficient point lookups etc

	dag, ok := p.remoteQueries[queryID]
	if !ok {
		var err error
		dag, err = p.getDagForRemoteQuery(schemaName, query)
		if err != nil {
			return nil, err
		}
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

func (p *PullEngine) getDagForRemoteQuery(schemaName string, query string) (exec.PullExecutor, error) {
	schema, ok := p.metaController.GetSchema(schemaName)
	if !ok {
		return nil, fmt.Errorf("no such schema %s", schemaName)
	}
	dag, err := p.ExecutePullQuery(schema, query)
	if err != nil {
		return nil, err
	}
	remExecutor := p.findRemoteExecutor(dag)
	if remExecutor == nil {
		return nil, errors.New("cannot find remote executor")
	}
	return remExecutor.RemoteDag, nil
}

func (p *PullEngine) findRemoteExecutor(executor exec.PullExecutor) *exec.RemoteExecutor {
	// We only execute the part of the dag beyond the table reader - this is the remote part
	remExecutor, ok := executor.(*exec.RemoteExecutor)
	if ok {
		return remExecutor
	}
	for _, child := range executor.GetChildren() {
		remExecutor := p.findRemoteExecutor(child)
		if remExecutor != nil {
			return remExecutor
		}
	}
	return nil
}
