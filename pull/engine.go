package pull

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"

	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/meta"
	"github.com/squareup/pranadb/parplan"
	"github.com/squareup/pranadb/pull/exec"
)

type PullEngine struct {
	lock            sync.RWMutex
	started         bool
	planner         *parplan.Planner
	remoteQueries   map[string]exec.PullExecutor
	cluster         cluster.Cluster
	metaController  *meta.Controller
	nodeID          int
	queryIDSequence int64
	queryLock       sync.Mutex
}

func NewPullEngine(planner *parplan.Planner, cluster cluster.Cluster, metaController *meta.Controller) *PullEngine {
	engine := PullEngine{
		planner:        planner,
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
	p.started = true
	return nil
}

func (p *PullEngine) Stop() {
	p.lock.Lock()
	defer p.lock.Unlock()
	if !p.started {
		return
	}
	p.started = false
}

func (p *PullEngine) BuildPullQuery(schema *common.Schema, query string) (queryDAG exec.PullExecutor, err error) {
	seq := atomic.AddInt64(&p.queryIDSequence, 1)
	queryID := fmt.Sprintf("%d-%d", p.nodeID, seq)
	return p.buildPullQueryExecution(schema, query, queryID, false, 0)
}

func (p *PullEngine) BuildRemotePullQuery(schema *common.Schema, query string, shardID uint64) (queryDAG exec.PullExecutor, err error) {
	return p.buildPullQueryExecution(schema, query, "", true, shardID)
}

func (p *PullEngine) ExecuteRemotePullQuery(schemaName string, query string, queryID string, limit int, shardID uint64) (*common.Rows, error) {
	// TODO one-shot optimisation - no need to register query

	p.lock.RLock()
	defer p.lock.RUnlock()

	// TODO prepared statements - we will need this for efficient point lookups etc

	log.Printf("Executing remote query in engine %s query id %s shardID %d", query, queryID, shardID)
	dag, ok := p.remoteQueries[queryID]
	if !ok {
		log.Println("Didn't find query in map, so creating a new dag")
		var err error
		dag, err = p.getDagForRemoteQuery(schemaName, query, shardID)
		if err != nil {
			return nil, err
		}
		p.remoteQueries[queryID] = dag
	}

	rows, err := dag.GetRows(limit)
	if err != nil {
		return nil, err
	}
	if rows.RowCount() == 0 {
		// TODO query timeouts
		p.deleteQuery(queryID)
	}
	log.Printf("Pull query %s on node %d and shard %d returning %d rows", query, p.cluster.GetNodeID(), shardID, rows.RowCount())
	return rows, err
}

func (p *PullEngine) deleteQuery(queryID string) {
	p.lock.RLock()
	defer p.lock.RUnlock()

	delete(p.remoteQueries, queryID)
}

func (p *PullEngine) getDagForRemoteQuery(schemaName string, query string, shardID uint64) (exec.PullExecutor, error) {
	schema, ok := p.metaController.GetSchema(schemaName)
	if !ok {
		return nil, fmt.Errorf("no such schema %s", schemaName)
	}
	dag, err := p.BuildRemotePullQuery(schema, query, shardID)
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
