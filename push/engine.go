package push

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/parplan"
	"github.com/squareup/pranadb/push/exec"
	"github.com/squareup/pranadb/sharder"
)

type PushEngine struct {
	lock              sync.RWMutex
	started           bool
	schedulers        map[uint64]*shardScheduler
	sources           map[uint64]*source
	materializedViews map[uint64]*materializedView
	remoteConsumers   map[uint64]*remoteConsumer
	forwardSequences  map[uint64]uint64
	localLeaderShards []uint64
	cluster           cluster.Cluster
	planner           *parplan.Planner
	sharder           *sharder.Sharder
	rnd               *rand.Rand
}

func NewPushEngine(cluster cluster.Cluster, planner *parplan.Planner, sharder *sharder.Sharder) *PushEngine {
	engine := PushEngine{
		remoteConsumers:   make(map[uint64]*remoteConsumer),
		sources:           make(map[uint64]*source),
		materializedViews: make(map[uint64]*materializedView),
		schedulers:        make(map[uint64]*shardScheduler),
		forwardSequences:  make(map[uint64]uint64),
		cluster:           cluster,
		planner:           planner,
		sharder:           sharder,
		rnd:               rand.New(rand.NewSource(time.Now().UTC().UnixNano())),
	}
	return &engine
}

// remoteConsumer is a wrapper for something that consumes rows that have arrived remotely from other shards
// e.g. a source or an aggregator
// TODO we don't need this if we pass the [][]byte straight to the handler in the source/mv
// which already knows these fields
type remoteConsumer struct {
	RowsFactory *common.RowsFactory
	ColTypes    []common.ColumnType
	RowsHandler remoteRowsHandler
}

// TODO do we even need these?
type remoteRowsHandler interface {
	HandleRemoteRows(rows *common.Rows, ctx *exec.ExecutionContext) error
}

type RawRowHandler interface {
	HandleRawRows(rawRows map[uint64][][]byte, batch *cluster.WriteBatch) error
}

func (p *PushEngine) Start() error {
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.started {
		return nil
	}
	err := p.checkForRowsToForward()
	if err != nil {
		return err
	}
	p.started = true
	return nil
}

func (p *PushEngine) Stop() {
	p.lock.Lock()
	defer p.lock.Unlock()
	if !p.started {
		return
	}
	for _, scheduler := range p.schedulers {
		scheduler.Stop()
	}
	p.started = false
}

func (p *PushEngine) LeaderChanged(shardID uint64, added bool) {
	p.lock.Lock()
	defer p.lock.Unlock()

	if added {
		_, ok := p.schedulers[shardID]
		if ok {
			panic("leader added but already leader")
		}
		sched := newShardScheduler(shardID, p)
		sched.Start()
		p.schedulers[shardID] = sched
		p.localLeaderShards = append(p.localLeaderShards, shardID)
	} else {
		sched := p.schedulers[shardID]
		sched.Stop()
		delete(p.schedulers, shardID)
		locShards := make([]uint64, len(p.localLeaderShards)-1)
		index := 0
		for _, sid := range p.localLeaderShards {
			if sid != shardID {
				locShards[index] = sid
				index++
			}
		}
		p.localLeaderShards = locShards
	}
	log.Printf("Calling leader changed, num local shards is now %d", len(p.localLeaderShards))
}

func (p *PushEngine) HandleRawRows(entityValues map[uint64][][]byte, batch *cluster.WriteBatch) error {
	log.Println("Handling remote rows")

	// TODO use copy on write and atomic references to entities to avoid read lock
	p.lock.RLock()
	defer p.lock.RUnlock()

	for entityID, rawRows := range entityValues {
		rc, ok := p.remoteConsumers[entityID]
		if !ok {
			return fmt.Errorf("entity with id %d not registered", entityID)
		}
		rows := rc.RowsFactory.NewRows(len(rawRows))
		for _, row := range rawRows {
			err := common.DecodeRow(row, rc.ColTypes, rows)
			if err != nil {
				return err
			}
		}
		execContext := &exec.ExecutionContext{
			WriteBatch: batch,
			Forwarder:  p,
		}
		log.Println("Sending to entity handler")
		err := rc.RowsHandler.HandleRemoteRows(rows, execContext)
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *PushEngine) RemoteWriteOccurred(shardID uint64) {
	err := p.remoteBatchArrived(shardID)
	if err != nil {
		log.Println(err)
	}
}

// IngestRows is only used in testing to inject rows
func (p *PushEngine) IngestRows(rows *common.Rows, sourceID uint64) error {
	source, ok := p.sources[sourceID]
	if !ok {
		return errors.New("no such source")
	}
	return p.ingest(rows, source)
}

// NumLocalLeaders is only used in tests
func (p *PushEngine) NumLocalLeaders() int {
	p.lock.Lock()
	defer p.lock.Unlock()
	return len(p.localLeaderShards)
}

func (p *PushEngine) ingest(rows *common.Rows, source *source) error {
	p.lock.Lock()

	var shardID uint64
	// Choose a shard, preferably local for storing this ingest
	numLocal := len(p.localLeaderShards)
	if numLocal > 0 {
		r := p.rnd.Int31n(int32(numLocal))
		shardID = p.localLeaderShards[r]
	} else {
		panic("no local shards")
	}

	scheduler := p.schedulers[shardID]
	p.lock.Unlock()

	errChan := scheduler.ScheduleAction(func() error {
		return source.ingestRows(rows, shardID)
	})

	err, ok := <-errChan
	if !ok {
		return errors.New("channel closed")
	}
	return err
}

// Triggered when some data is written into the forward queue of a shard for which this node
// is a leader. In this case we want to deliver that remote batch to any interested parties
func (p *PushEngine) remoteBatchArrived(shardID uint64) error {
	p.lock.RLock()
	defer p.lock.RUnlock()
	// We maintain one goroutine for processing of data per shard - the shard defines the
	// granularity of parallelism
	scheduler, ok := p.schedulers[shardID]
	if !ok {
		scheduler = newShardScheduler(shardID, p)
		p.schedulers[shardID] = scheduler
		p.localLeaderShards = append(p.localLeaderShards, shardID)
		scheduler.Start()
	}
	if !ok {
		return fmt.Errorf("no scheduler for shard %d", shardID)
	}
	scheduler.CheckForRemoteBatch()
	return nil
}

func (p *PushEngine) checkForRowsToForward() error {
	// If the node failed previously there may be un-forwarded rows in the forwarder table
	// If there are we need to forwarded them
	for _, scheduler := range p.schedulers {
		ch := scheduler.CheckForRowsToForward()
		err, ok := <-ch
		if !ok {
			return errors.New("channel was closed")
		}
		if err != nil {
			return err
		}
	}
	return nil
}
