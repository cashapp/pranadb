package push

import (
	"errors"
	"fmt"
	"github.com/squareup/pranadb/perrors"
	"math/rand"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/conf"
	"github.com/squareup/pranadb/protolib"
	"github.com/squareup/pranadb/push/mover"
	"github.com/squareup/pranadb/push/sched"
	"github.com/squareup/pranadb/push/source"

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
	localShardsLock   sync.RWMutex
	started           bool
	schedulers        map[uint64]*sched.ShardScheduler
	sources           map[uint64]*source.Source
	materializedViews map[uint64]*MaterializedView
	remoteConsumers   map[uint64]*RemoteConsumer
	mover             *mover.Mover
	localLeaderShards []uint64
	cluster           cluster.Cluster
	sharder           *sharder.Sharder
	meta              *meta.Controller
	rnd               *rand.Rand
	cfg               *conf.Config
	queryExec         common.SimpleQueryExec
	protoRegistry     protolib.Resolver
	readyToReceive    common.AtomicBool
}

// RemoteConsumer is a wrapper for something that consumes rows that have arrived remotely from other shards
// e.g. a source or an aggregator
// TODO we don't need this if we pass the [][]byte straight to the handler in the source/mv
// which already knows these fields
type RemoteConsumer struct {
	RowsFactory *common.RowsFactory
	ColTypes    []common.ColumnType
	RowsHandler remoteRowsHandler
}

type shardListener struct {
	shardID uint64
	p       *PushEngine
	sched   *sched.ShardScheduler
}

// TODO do we even need these?
type remoteRowsHandler interface {
	HandleRemoteRows(rowsBatch exec.RowsBatch, ctx *exec.ExecutionContext) error
}

func NewPushEngine(cluster cluster.Cluster, sharder *sharder.Sharder, meta *meta.Controller, cfg *conf.Config, queryExec common.SimpleQueryExec, registry protolib.Resolver) *PushEngine {
	engine := PushEngine{
		mover:         mover.NewMover(cluster),
		cluster:       cluster,
		sharder:       sharder,
		meta:          meta,
		rnd:           rand.New(rand.NewSource(time.Now().UTC().UnixNano())),
		cfg:           cfg,
		queryExec:     queryExec,
		protoRegistry: registry,
	}
	engine.createMaps()
	return &engine
}

func (p *PushEngine) Start() error {
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.started {
		return nil
	}
	p.started = true
	return nil
}

// Ready signals that the push engine is now ready to receive any incoming data
func (p *PushEngine) Ready() error {
	p.readyToReceive.Set(true)
	return p.checkForPendingData()
}

func (p *PushEngine) Stop() error {
	p.lock.Lock()
	defer p.lock.Unlock()
	if !p.started {
		return nil
	}
	p.readyToReceive.Set(false)
	for _, src := range p.sources {
		if err := src.Stop(); err != nil {
			return err
		}
	}
	for _, sh := range p.schedulers {
		sh.Stop()
	}
	p.createMaps() // Clear the internal state
	p.started = false
	return nil
}

func (p *PushEngine) GetSource(sourceID uint64) (*source.Source, error) {
	p.lock.RLock()
	defer p.lock.RUnlock()
	source, ok := p.sources[sourceID]
	if !ok {
		return nil, errors.New("no such source")
	}
	return source, nil
}

func (p *PushEngine) RemoveSource(sourceInfo *common.SourceInfo) (*source.Source, error) {
	p.lock.Lock()
	defer p.lock.Unlock()

	src, ok := p.sources[sourceInfo.ID]
	if !ok {
		return nil, perrors.Errorf("no such source %d", sourceInfo.ID)
	}
	if src.IsRunning() {
		return nil, errors.New("source is running")
	}

	delete(p.sources, sourceInfo.ID)
	delete(p.remoteConsumers, sourceInfo.ID)

	return src, nil
}

func (p *PushEngine) RegisterRemoteConsumer(id uint64, rc *RemoteConsumer) error {
	p.lock.Lock()
	defer p.lock.Unlock()

	if _, ok := p.remoteConsumers[id]; ok {
		return perrors.Errorf("remote consumer with id %d already registered", id)
	}
	p.remoteConsumers[id] = rc
	return nil
}

func (p *PushEngine) UnregisterRemoteConsumer(id uint64) error {
	p.lock.Lock()
	defer p.lock.Unlock()

	_, ok := p.remoteConsumers[id]
	if !ok {
		return perrors.Errorf("remote consumer with id %d not registered", id)
	}
	delete(p.remoteConsumers, id)
	return nil
}

func (p *PushEngine) GetMaterializedView(mvID uint64) (*MaterializedView, error) {
	p.lock.RLock()
	defer p.lock.RUnlock()
	mv, ok := p.materializedViews[mvID]
	if !ok {
		return nil, perrors.Errorf("no such materialized view %d", mvID)
	}
	return mv, nil
}

func (p *PushEngine) RemoveMV(mvID uint64) error {
	p.lock.Lock()
	defer p.lock.Unlock()
	_, ok := p.materializedViews[mvID]
	if !ok {
		return perrors.Errorf("cannot find materialized view with id %d", mvID)
	}
	delete(p.materializedViews, mvID)
	return nil
}

func (p *PushEngine) RegisterMV(mv *MaterializedView) error {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.materializedViews[mv.Info.TableInfo.ID] = mv
	return nil
}

func (p *PushEngine) CreateShardListener(shardID uint64) cluster.ShardListener {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.localShardsLock.Lock()
	defer p.localShardsLock.Unlock()
	sh := sched.NewShardScheduler(shardID)
	sh.Start()
	p.schedulers[shardID] = sh
	p.localLeaderShards = append(p.localLeaderShards, shardID)
	return &shardListener{
		shardID: shardID,
		p:       p,
		sched:   sh,
	}
}

func (s *shardListener) RemoteWriteOccurred() {
	if !s.p.readyToReceive.Get() {
		return
	}
	s.scheduleHandleRemoteBatch()
}

func (s *shardListener) scheduleHandleRemoteBatch() {
	s.p.maybeHandleRemoteBatch(s.sched)
}

func (p *PushEngine) maybeHandleRemoteBatch(scheduler *sched.ShardScheduler) {
	scheduler.ScheduleActionFireAndForget(func() error {
		hasForwards, err := p.mover.HandleReceivedRows(scheduler.ShardID(), p)
		if err != nil {
			return err
		}
		if hasForwards {
			return p.mover.TransferData(scheduler.ShardID(), true)
		}
		return nil
	})
}

func (s *shardListener) Close() {
	s.sched.Stop()
	s.p.removeScheduler(s.shardID)
	s.p.localShardsLock.Lock()
	defer s.p.localShardsLock.Unlock()
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

func (p *PushEngine) removeScheduler(shardID uint64) {
	p.lock.Lock()
	defer p.lock.Unlock()
	delete(p.schedulers, shardID)
}

func (p *PushEngine) HandleRawRows(entityValues map[uint64][][]byte, batch *cluster.WriteBatch) error {
	// TODO use copy on write and atomic references to entities to avoid read lock
	p.lock.RLock()
	defer p.lock.RUnlock()

	for entityID, rawRows := range entityValues {

		remoteConsumer, ok := p.remoteConsumers[entityID]
		if !ok {
			// Does the entity exist in storage?
			rows, err := p.queryExec.ExecuteQuery("sys", fmt.Sprintf("select id, prepare_state from tables where id=%d", entityID))
			if err != nil {
				return err
			}
			if rows.RowCount() == 1 {
				// The entity is in storage but not deployed - this might happen if a node joined when a create source/mv
				// was in progress so did not get the notifications but did see it in storage - in this case
				// we periodically scan sys.tables to check for any non registered entities TODO
				return perrors.Errorf("entity with id %d not registered", entityID)
			}
			// The entity does not exist in storage - it must correspond to a dropped entity - we can ignore the row
			// and it will get deleted from the receiver table
			log.Warnf("Received rows - Entity with id %d is not registered and does not exist in storage. Will be ignored as likely corresponds to a dropped source or materialized view.", entityID)
			continue
		}

		rows := remoteConsumer.RowsFactory.NewRows(len(rawRows))
		entries := make([]exec.RowsEntry, len(rawRows))
		rc := 0
		for i, row := range rawRows {
			lpvb, _ := common.ReadUint32FromBufferLE(row, 0)
			pi := -1
			if lpvb != 0 {
				prevBytes := row[4 : 4+lpvb]
				if err := common.DecodeRow(prevBytes, remoteConsumer.ColTypes, rows); err != nil {
					return err
				}
				pi = rc
				rc++
			}
			lcvb, _ := common.ReadUint32FromBufferLE(row, int(4+lpvb))
			ci := -1
			if lcvb != 0 {
				currBytes := row[8+lpvb:]
				if err := common.DecodeRow(currBytes, remoteConsumer.ColTypes, rows); err != nil {
					return err
				}
				ci = rc
				rc++
			}
			entries[i] = exec.NewRowsEntry(pi, ci)
		}
		execContext := &exec.ExecutionContext{
			WriteBatch: batch,
			Mover:      p.mover,
		}
		rowsBatch := exec.NewRowsBatch(rows, entries)
		err := remoteConsumer.RowsHandler.HandleRemoteRows(rowsBatch, execContext)
		if err != nil {
			return err
		}
	}
	return nil
}

// ChooseLocalScheduler chooses a local scheduler by hashing the key
func (p *PushEngine) ChooseLocalScheduler(key []byte) (*sched.ShardScheduler, error) {
	p.localShardsLock.RLock()
	defer p.localShardsLock.RUnlock()
	if len(p.localLeaderShards) == 0 {
		return nil, errors.New("no local leader shards")
	}
	shardID, err := p.sharder.CalculateShardWithShardIDs(sharder.ShardTypeHash, key, p.localLeaderShards)
	if err != nil {
		return nil, err
	}
	return p.schedulers[shardID], nil
}

func (p *PushEngine) checkForPendingData() error {
	// If the node failed previously or received messages it was unable to handle as it was starting up then
	// there could be rows in the forwarder table that need to be handled or incoming rows in the receiver table
	// we check and process these, if there are any
	for _, scheduler := range p.schedulers {
		ch := scheduler.ScheduleAction(func() error {
			return p.mover.TransferData(scheduler.ShardID(), true)
		})
		err, ok := <-ch
		if !ok {
			return errors.New("channel was closed")
		}
		if err != nil {
			return err
		}
		p.maybeHandleRemoteBatch(scheduler)
	}
	return nil
}

// WaitForProcessingToComplete is used in tests to wait for all rows have been processed when ingesting test data
func (p *PushEngine) WaitForProcessingToComplete() error {

	err := p.waitForSchedulers()
	if err != nil {
		return err
	}

	// Wait for no rows in the forwarder table
	err = p.waitForNoRowsInTable(common.ForwarderTableID)
	if err != nil {
		return err
	}

	// Wait for no rows in the receiver table
	err = p.waitForNoRowsInTable(common.ReceiverTableID)
	if err != nil {
		return err
	}

	return nil
}

func (p *PushEngine) waitForSchedulers() error {
	p.lock.RLock()
	defer p.lock.RUnlock()

	// Wait for schedulers to complete processing anything they're doing
	chans := make([]chan struct{}, 0, len(p.schedulers))
	for _, sched := range p.schedulers {
		ch := make(chan struct{}, 1)
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
		exist, err := p.ExistRowsInLocalTable(tableID, shardIDs)
		return !exist, err
	}, 30*time.Second, 100*time.Millisecond)
	if !ok {
		return errors.New("timed out waiting for condition")
	}
	return err
}

func (p *PushEngine) ExistRowsInLocalTable(tableID uint64, localShards []uint64) (bool, error) {
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

func (p *PushEngine) VerifyNoSourcesOrMVs() error {
	if len(p.sources) > 0 {
		return perrors.Errorf("there is %d source", len(p.sources))
	}
	if len(p.materializedViews) > 0 {
		return perrors.Errorf("there is %d materialized view", len(p.materializedViews))
	}
	return nil
}

func (p *PushEngine) GetScheduler(shardID uint64) (*sched.ShardScheduler, bool) {
	p.lock.RLock()
	defer p.lock.RUnlock()
	sched, ok := p.schedulers[shardID]
	return sched, ok
}

func (p *PushEngine) CreateSource(sourceInfo *common.SourceInfo) (*source.Source, error) {

	p.lock.Lock()
	defer p.lock.Unlock()

	tableExecutor := exec.NewTableExecutor(sourceInfo.TableInfo, p.cluster)

	src, err := source.NewSource(
		sourceInfo,
		tableExecutor,
		p.sharder,
		p.cluster,
		p.mover,
		p,
		p.cfg,
		p.queryExec,
		p.protoRegistry,
	)
	if err != nil {
		return nil, err
	}

	colTypes := sourceInfo.TableInfo.ColumnTypes
	rf := common.NewRowsFactory(colTypes)
	rc := &RemoteConsumer{
		RowsFactory: rf,
		ColTypes:    colTypes,
		RowsHandler: src.TableExecutor(),
	}
	p.remoteConsumers[sourceInfo.TableInfo.ID] = rc

	p.sources[sourceInfo.TableInfo.ID] = src
	return src, nil
}

func (p *PushEngine) Mover() *mover.Mover {
	return p.mover
}

func (p *PushEngine) createMaps() {
	p.remoteConsumers = make(map[uint64]*RemoteConsumer)
	p.sources = make(map[uint64]*source.Source)
	p.materializedViews = make(map[uint64]*MaterializedView)
	p.schedulers = make(map[uint64]*sched.ShardScheduler)
}

func (p *PushEngine) GetLocalLeaderSchedulers() (map[uint64]*sched.ShardScheduler, error) {
	p.lock.RLock()
	defer p.lock.RUnlock()
	schedulers := make(map[uint64]*sched.ShardScheduler, len(p.localLeaderShards))
	for _, lls := range p.localLeaderShards {
		sched, ok := p.schedulers[lls]
		if !ok {
			return nil, perrors.Errorf("no scheduler for local leader shard %d", lls)
		}
		schedulers[lls] = sched
	}
	return schedulers, nil
}

func (p *PushEngine) IsEmpty() bool {
	p.lock.Lock()
	defer p.lock.Unlock()
	return len(p.sources) == 0 && len(p.materializedViews) == 0 && len(p.remoteConsumers) == 0
}
