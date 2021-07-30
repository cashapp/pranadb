package push

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/squareup/pranadb/meta"
	"github.com/squareup/pranadb/table"

	"github.com/squareup/pranadb/common/commontest"

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
	meta              *meta.Controller
	rnd               *rand.Rand
	runningSchedulers int32
}

func NewPushEngine(cluster cluster.Cluster, sharder *sharder.Sharder, meta *meta.Controller) *PushEngine {
	engine := PushEngine{
		remoteConsumers:   make(map[uint64]*remoteConsumer),
		sources:           make(map[uint64]*source),
		materializedViews: make(map[uint64]*materializedView),
		schedulers:        make(map[uint64]*shardScheduler),
		forwardSequences:  make(map[uint64]uint64),
		cluster:           cluster,
		sharder:           sharder,
		meta:              meta,
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

func (p *PushEngine) Stop() error {
	p.lock.Lock()
	defer p.lock.Unlock()
	if !p.started {
		return nil
	}
	for _, sched := range p.schedulers {
		sched.Stop()
	}
	err := p.waitUntilNoSchedulersRunning()
	if err != nil {
		return err
	}
	p.started = false
	return nil
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
	shardIDs := p.cluster.GetLocalShardIDs()
	ok, err := commontest.WaitUntilWithError(func() (bool, error) {
		exist, err := p.existRowsInLocalTable(tableID, shardIDs)
		return !exist, err
	}, 5*time.Second, 10*time.Millisecond)
	if !ok {
		return errors.New("timed out waiting for condition")
	}
	return err
}

func (p *PushEngine) existRowsInLocalTable(tableID uint64, localShards []uint64) (bool, error) {
	for _, shardID := range localShards {
		startPrefix := table.EncodeTableKeyPrefix(tableID, shardID, 16)
		endPrefix := table.EncodeTableKeyPrefix(tableID+1, shardID, 16)
		kvPairs, err := p.cluster.LocalScan(startPrefix, endPrefix, 1)
		if err != nil {
			return false, err
		}
		if kvPairs != nil {
			return true, nil
		}
	}
	return false, nil
}

func (p *PushEngine) waitUntilNoSchedulersRunning() error {
	start := time.Now()
	for {
		numSchedulers := atomic.LoadInt32(&p.runningSchedulers)
		if numSchedulers == 0 {
			return nil
		}
		time.Sleep(time.Millisecond)
		if time.Now().Sub(start) >= 5*time.Second {
			return fmt.Errorf("timed out waiting for schedulers to stop, sched count is %d", numSchedulers)
		}
	}
}

func (p *PushEngine) VerifyNoSourcesOrMVs() error {
	if len(p.sources) > 0 {
		return fmt.Errorf("there is %d source", len(p.sources))
	}
	if len(p.materializedViews) > 0 {
		return fmt.Errorf("there is %d materialized view", len(p.materializedViews))
	}
	return nil
}

func (p *PushEngine) RemoveSource(sourceID uint64, deleteData bool) error {
	p.lock.Lock()
	defer p.lock.Unlock()

	delete(p.sources, sourceID)
	delete(p.remoteConsumers, sourceID)

	if deleteData {
		return p.deleteAllDataForTable(sourceID)
	}
	return nil
}

func (p *PushEngine) RemoveMV(schema *common.Schema, info *common.MaterializedViewInfo, deleteData bool) error {
	p.lock.Lock()
	defer p.lock.Unlock()

	mvID := info.TableInfo.ID
	mv, ok := p.materializedViews[mvID]
	if !ok {
		return fmt.Errorf("cannot find materialized view with id %d", mvID)
	}
	err := p.disconnectMV(schema, mv.tableExecutor, deleteData)
	if err != nil {
		return err
	}
	delete(p.materializedViews, mvID)

	if deleteData {
		return p.deleteAllDataForTable(mvID)
	}
	return nil
}

func (p *PushEngine) disconnectMV(schema *common.Schema, node exec.PushExecutor, deleteData bool) error {

	switch op := node.(type) {
	case *exec.TableScan:
		tableName := op.TableName
		tbl, ok := schema.GetTable(tableName)
		if !ok {
			return fmt.Errorf("unknown source or materialized view %s", tableName)
		}
		switch tbl := tbl.(type) {
		case *common.SourceInfo:
			source := p.sources[tbl.ID]
			source.removeConsumingExecutor(node)
		case *common.MaterializedViewInfo:
			mv := p.materializedViews[tbl.ID]
			mv.removeConsumingExecutor(node)
		default:
			return fmt.Errorf("cannot disconnect %s: invalid table type", tbl)
		}
	case *exec.Aggregator:
		delete(p.remoteConsumers, op.AggTableInfo.ID)
		if deleteData {
			err := p.deleteAllDataForTable(op.AggTableInfo.ID)
			if err != nil {
				return err
			}
		}
	}

	for _, child := range node.GetChildren() {
		err := p.disconnectMV(schema, child, deleteData)
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *PushEngine) deleteAllDataForTable(tableID uint64) error {
	startPrefix := common.AppendUint64ToBufferBE([]byte{}, tableID)
	endPrefix := common.AppendUint64ToBufferBE([]byte{}, tableID+1)
	return p.cluster.DeleteAllDataInRange(startPrefix, endPrefix)
}
