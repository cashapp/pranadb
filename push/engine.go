package push

import (
	"errors"
	"fmt"
	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/hash"
	"github.com/squareup/pranadb/parplan"
	"github.com/squareup/pranadb/push/exec"
	"github.com/squareup/pranadb/storage"
	"log"
	"sync"
)

type PushEngine struct {
	lock              sync.RWMutex
	started           bool
	mover             *mover
	storage           storage.Storage
	schedulers        map[uint64]*shardScheduler
	sources           map[uint64]*source
	materializedViews map[uint64]*materializedView
	remoteConsumers   map[uint64]*remoteConsumer
	allShards         []uint64
	cluster           cluster.Cluster
	planner           *parplan.Planner
}

func NewPushEngine(storage storage.Storage, cluster cluster.Cluster, planner *parplan.Planner) *PushEngine {
	engine := PushEngine{
		mover:           nil,
		remoteConsumers: make(map[uint64]*remoteConsumer),
		sources:         make(map[uint64]*source),
		schedulers:      make(map[uint64]*shardScheduler),
		cluster:         cluster,
		storage:         storage,
		planner:         planner,
	}
	engine.mover = newMover(storage, &engine, &engine)
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
	HandleRows(rows *common.Rows, ctx *exec.ExecutionContext) error
}

type receiverHandler interface {
	HandleRemoteRows(entityValues map[uint64][][]byte, batch *storage.WriteBatch) error
}

func (p *PushEngine) Start() error {
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.started {
		return nil
	}

	err := p.loadAndStartSchedulers()
	if err != nil {
		return err
	}

	err = p.loadAllShards()
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

	for _, scheduler := range p.schedulers {
		scheduler.Stop()
	}

	p.started = false
}

// Load and start a scheduler for every shard for which we are a leader
// These are used to deliver any incoming rows for that shard
func (p *PushEngine) loadAndStartSchedulers() error {
	nodeInfo, err := p.cluster.GetNodeInfo(p.cluster.GetNodeID())
	if err != nil {
		return err
	}
	for _, shardID := range nodeInfo.Leaders {
		scheduler := NewShardScheduler(shardID, p.mover, p.storage)
		p.schedulers[shardID] = scheduler
		scheduler.Start()
	}
	return nil
}

func (p *PushEngine) loadAllShards() error {
	clusterInfo, err := p.cluster.GetClusterInfo()
	if err != nil {
		return err
	}
	for _, nodeInfo := range clusterInfo.NodeInfos {
		for _, leader := range nodeInfo.Leaders {
			p.allShards = append(p.allShards, leader)
		}
	}
	return nil
}

func (p *PushEngine) HandleRemoteRows(entityValues map[uint64][][]byte, batch *storage.WriteBatch) error {

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
			Forwarder:  p.mover,
		}
		log.Println("Sending to entity handler")
		err := rc.RowsHandler.HandleRows(rows, execContext)
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
func (p *PushEngine) IngestRows(sourceID uint64, rows *common.Rows) error {
	source, ok := p.sources[sourceID]
	if !ok {
		return errors.New("no such source")
	}
	return source.ingestRows(rows)
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

func (p *PushEngine) CalculateShard(key []byte) (uint64, error) {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return hash.ComputeShard(key, p.allShards), nil
}
