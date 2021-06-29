package pull

import (
	"github.com/pingcap/tidb/infoschema"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/parplan"
	"github.com/squareup/pranadb/pull/exec"
	"sync"
)

type PullEngine struct {
	lock    sync.RWMutex
	started bool
	planner *parplan.Planner
}

func NewPullEngine(planner *parplan.Planner) *PullEngine {
	engine := PullEngine{
		planner: planner,
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

func (p *PullEngine) ExecutePullQuery(schema *common.Schema, is infoschema.InfoSchema, query string) (queryDAG exec.PullExecutor, err error) {
	return p.buildPullQueryExecution(schema, is, query)
}
