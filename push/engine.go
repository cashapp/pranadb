package push

import (
	"errors"
	"fmt"
	"github.com/squareup/pranadb/common/commontest"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/common"
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
	sharder           *sharder.Sharder
	rnd               *rand.Rand
}

func NewPushEngine(cluster cluster.Cluster, sharder *sharder.Sharder) *PushEngine {
	engine := PushEngine{
		remoteConsumers:   make(map[uint64]*remoteConsumer),
		sources:           make(map[uint64]*source),
		materializedViews: make(map[uint64]*materializedView),
		schedulers:        make(map[uint64]*shardScheduler),
		forwardSequences:  make(map[uint64]uint64),
		cluster:           cluster,
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
	// We don't stop the schedulers here - their close is controlled by ShardListener
	p.started = false
}

func (p *PushEngine) CreateShardListener(shardID uint64) cluster.ShardListener {
	p.lock.Lock()
	defer p.lock.Unlock()
	sched := newShardScheduler(shardID, p)
	sched.Start()
	p.schedulers[shardID] = sched
	p.localLeaderShards = append(p.localLeaderShards, shardID)
	return &shardListener{
		shardID: shardID,
		p:       p,
		sched:   sched,
	}
}

type shardListener struct {
	shardID uint64
	p       *PushEngine
	sched   *shardScheduler
}

func (s *shardListener) RemoteWriteOccurred() {
	s.sched.CheckForRemoteBatch()
}

func (s *shardListener) Close() {
	s.p.lock.Lock()
	defer s.p.lock.Unlock()
	s.sched.Stop()
	delete(s.p.schedulers, s.shardID)
	locShards := make([]uint64, len(s.p.localLeaderShards)-1)
	index := 0
	for _, sid := range s.p.localLeaderShards {
		if sid != s.shardID {
			locShards[index] = sid
			index++
		}
	}
	s.p.localLeaderShards = locShards
}

func (p *PushEngine) HandleRawRows(entityValues map[uint64][][]byte, batch *cluster.WriteBatch) error {
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
		err := rc.RowsHandler.HandleRemoteRows(rows, execContext)
		if err != nil {
			return err
		}
	}
	return nil
}

// IngestRows is only used in testing to inject rows
func (p *PushEngine) IngestRows(rows *common.Rows, sourceID uint64) error {
	source, ok := p.sources[sourceID]
	if !ok {
		return errors.New("no such source")
	}
	return p.ingest(rows, source)
}

func (p *PushEngine) ingest(rows *common.Rows, source *source) error {
	scheduler, shardID := p.chooseScheduler()
	errChan := scheduler.ScheduleAction(func() error {
		return source.ingestRows(rows, shardID)
	})
	err, ok := <-errChan
	if !ok {
		return errors.New("channel closed")
	}
	return err
}

func (p *PushEngine) chooseScheduler() (*shardScheduler, uint64) {
	p.lock.Lock()
	defer p.lock.Unlock()

	var shardID uint64
	// Choose a shard, preferably local for storing this ingest
	numLocal := len(p.localLeaderShards)
	if numLocal > 0 {
		r := p.rnd.Int31n(int32(numLocal))
		shardID = p.localLeaderShards[r]
	} else {
		panic("no local shards")
	}
	return p.schedulers[shardID], shardID
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

// WaitForProcessingToComplete is used in tests to wait for all rows have been processed when ingesting test data
func (p *PushEngine) WaitForProcessingToComplete() error {

	log.Printf("Waiting for processing to complete on node %d", p.cluster.GetNodeID())

	err := p.waitForSchedulers()
	if err != nil {
		return err
	}

	log.Printf("Schedulers complete on node %d", p.cluster.GetNodeID())

	// Wait for no rows in the forwarder table
	err = p.waitForNoRowsInTable(common.ForwarderTableID)
	if err != nil {
		return err
	}

	log.Printf("No rows in forwarder table on node %d", p.cluster.GetNodeID())

	// Wait for no rows in the receiver table
	err = p.waitForNoRowsInTable(common.ReceiverTableID)
	if err != nil {
		return err
	}

	log.Printf("No rows in receiver table on node %d", p.cluster.GetNodeID())
	return nil
}

func (p *PushEngine) waitForSchedulers() error {
	p.lock.RLock()
	defer p.lock.RUnlock()

	// Wait for schedulers to complete processing anything they're doing
	chans := make([]chan struct{}, 0, len(p.schedulers))
	for _, sched := range p.schedulers {
		ch := make(chan struct{})
		chans = append(chans, ch)
		sched.ScheduleAction(func() error {
			ch <- struct{}{}
			return nil
		})
	}
	for _, ch := range chans {
		_, ok := <-ch
		if !ok {
			return errors.New("chan was closed")
		}
	}
	return nil
}

func (p *PushEngine) waitForNoRowsInTable(tableID uint64) error {
	ok, err := commontest.WaitUntilWithError(func() (bool, error) {
		keyStartPrefix := make([]byte, 0, 16)
		keyStartPrefix = common.AppendUint64ToBufferLittleEndian(keyStartPrefix, tableID)
		kvPairs, err := p.cluster.LocalScan(keyStartPrefix, keyStartPrefix, 1)
		if err != nil {
			return false, err
		}
		return len(kvPairs) == 0, nil
	}, 5*time.Second, 10*time.Millisecond)
	if !ok {
		return errors.New("timed out waiting for condition")
	}
	if err != nil {
		return err
	}
	return nil
}
