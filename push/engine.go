package push

import (
	"errors"
	"fmt"
	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/parplan"
	"github.com/squareup/pranadb/push/exec"
	"github.com/squareup/pranadb/sharder"
	"github.com/squareup/pranadb/storage"
	"log"
	"sync"
	"sync/atomic"
)

type PushEngine struct {
	lock                sync.RWMutex
	started             bool
	storage             storage.Storage
	schedulers          map[uint64]*shardScheduler
	sources             map[uint64]*source
	materializedViews   map[uint64]*materializedView
	remoteConsumers     map[uint64]*remoteConsumer
	localShards         []uint64
	cluster             cluster.Cluster
	planner             *parplan.Planner
	sharder             *sharder.Sharder
	nextLocalShardIndex int64
}

func NewPushEngine(storage storage.Storage, cluster cluster.Cluster, planner *parplan.Planner, sharder *sharder.Sharder) *PushEngine {
	engine := PushEngine{
		remoteConsumers:   make(map[uint64]*remoteConsumer),
		sources:           make(map[uint64]*source),
		materializedViews: make(map[uint64]*materializedView),
		schedulers:        make(map[uint64]*shardScheduler),
		cluster:           cluster,
		storage:           storage,
		planner:           planner,
		sharder:           sharder,
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

func (p *PushEngine) Start() error {
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.started {
		return nil
	}
	p.cluster.SetLeaderChangedCallback(p)
	err := p.loadAndStartSchedulers()
	if err != nil {
		return err
	}
	p.storage.SetRemoteWriteHandler(p)
	p.started = true
	return nil
}

func (p *PushEngine) Stop() {
	p.lock.Lock()
	defer p.lock.Unlock()
	if !p.started {
		return
	}
	p.cluster.SetLeaderChangedCallback(nil)
	for _, scheduler := range p.schedulers {
		scheduler.Stop()
	}
	p.started = false
}

func (p *PushEngine) LeaderChanged(shardID uint64, added bool) {
	// TODO
	// This will be called from the cluster manager when the local node becomes the leader
	// for a shard. This can happen if another node leaves the cluster.
	// TODO - do we really need this? If a remote write comes in for a shard and we don't
	// already have a shard scheduler we can lazily create one. In fact we don't have
	// to create shard schedulers at all at startup - they can all be created lazily
}

// Load and start a scheduler for every shard for which we are a leader
// These are used to deliver any incoming rows for that shard
func (p *PushEngine) loadAndStartSchedulers() error {
	nodeInfo, err := p.cluster.GetNodeInfo(p.cluster.GetNodeID())
	if err != nil {
		return err
	}
	for _, shardID := range nodeInfo.Leaders {
		scheduler := NewShardScheduler(shardID, p)
		p.schedulers[shardID] = scheduler
		scheduler.Start()
		p.localShards = append(p.localShards, shardID)
	}
	return nil
}

func (p *PushEngine) HandleRawRows(entityValues map[uint64][][]byte, batch *storage.WriteBatch) error {
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

// Only used in testing to inject rows
func (p *PushEngine) IngestRows(rows *common.Rows, sourceID uint64) error {
	source, ok := p.sources[sourceID]
	if !ok {
		return errors.New("no such source")
	}
	return p.ingest(rows, source)
}

func (p *PushEngine) ingest(rows *common.Rows, source *source) error {
	// We choose a local shard to handle this batch - rows are written into forwarder queue
	// and forwarded to dest shards
	shardID := p.getLocalShardRoundRobin()
	scheduler := p.schedulers[shardID]

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
		return fmt.Errorf("no scheduler for shard %d", shardID)
	}
	scheduler.CheckForRemoteBatch()
	return nil
}

func genLocalShards(schedulers map[uint64]*shardScheduler) []uint64 {
	var localShards []uint64
	for shardID, _ := range schedulers {
		localShards = append(localShards, shardID)
	}
	return localShards
}

func (p *PushEngine) getLocalShardRoundRobin() uint64 {
	shardIndex := atomic.AddInt64(&p.nextLocalShardIndex, 1)
	if shardIndex < 0 {
		// Doesn't matter if race here, this is best effort
		atomic.StoreInt64(&p.nextLocalShardIndex, 0)
	}
	return p.localShards[shardIndex]
}
