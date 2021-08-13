package push

import (
	"errors"
	"fmt"
	"github.com/squareup/pranadb/conf"
	"github.com/squareup/pranadb/push/mover"
	"github.com/squareup/pranadb/push/sched"
	"github.com/squareup/pranadb/push/source"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/squareup/pranadb/meta"
	"github.com/squareup/pranadb/table"

	"github.com/squareup/pranadb/common/commontest"

	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/push/exec"
	"github.com/squareup/pranadb/sharder"
)

const remoteBatchRetryDelay = 2 * time.Second

type PushEngine struct {
	lock              sync.RWMutex
	localShardsLock   sync.RWMutex
	started           bool
	schedulers        map[uint64]*sched.ShardScheduler
	sources           map[uint64]*source.Source
	materializedViews map[uint64]*materializedView
	remoteConsumers   map[uint64]*remoteConsumer
	mover             *mover.Mover
	localLeaderShards []uint64
	cluster           cluster.Cluster
	sharder           *sharder.Sharder
	meta              *meta.Controller
	rnd               *rand.Rand
	cfg               *conf.Config
	queryExec         common.SimpleQueryExec
}

func NewPushEngine(cluster cluster.Cluster, sharder *sharder.Sharder, meta *meta.Controller, cfg *conf.Config,
	queryExec common.SimpleQueryExec) *PushEngine {
	engine := PushEngine{
		mover:     mover.NewMover(cluster),
		cluster:   cluster,
		sharder:   sharder,
		meta:      meta,
		rnd:       rand.New(rand.NewSource(time.Now().UTC().UnixNano())),
		cfg:       cfg,
		queryExec: queryExec,
	}
	engine.createMaps()
	return &engine
}

func (p *PushEngine) createMaps() {
	p.remoteConsumers = make(map[uint64]*remoteConsumer)
	p.sources = make(map[uint64]*source.Source)
	p.materializedViews = make(map[uint64]*materializedView)
	p.schedulers = make(map[uint64]*sched.ShardScheduler)
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
	for _, src := range p.sources {
		if err := src.Stop(); err != nil {
			return err
		}
	}
	for _, sched := range p.schedulers {
		sched.Stop()
	}
	p.createMaps() // Clear the internal state
	p.started = false
	return nil
}

func (p *PushEngine) CreateShardListener(shardID uint64) cluster.ShardListener {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.localShardsLock.Lock()
	defer p.localShardsLock.Unlock()
	sched := sched.NewShardScheduler(shardID)
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
	sched   *sched.ShardScheduler
}

// RemoteWriteOccurred TODO we should also periodically call maybeHandleRemoteBatch as the call can fail if there
// is no remote entity registered, i.e. the source or aggregate table hasn't been registered yet.
// This could happen at startup when data is forwarded to a node but it hasn't completed loading all entities at startup
// yet. In this case we want to retry
func (s *shardListener) RemoteWriteOccurred() {
	s.scheduleHandleRemoteBatch()
}

func (s *shardListener) scheduleHandleRemoteBatch() {
	s.sched.ScheduleActionFireAndForget(s.maybeHandleRemoteBatch)
}

func (s *shardListener) maybeHandleRemoteBatch() error {
	err := s.p.mover.HandleReceivedRows(s.shardID, s.p)
	if err != nil {
		// It's possible an error can occur in handling received rows if the source or aggregate table is not
		// yet registered - this could be the case if rows are forwarded right after startup - in this case we can just
		// retry
		log.Printf("failed to handle received rows %v will retry after delay", err)
		time.AfterFunc(remoteBatchRetryDelay, func() {
			if err := s.maybeHandleRemoteBatch(); err != nil {
				log.Printf("failed to process remote batch %v", err)
			}
		})
		return nil
	}
	return s.p.mover.TransferData(s.shardID, true)
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
			Mover:      p.mover,
		}
		err := rc.RowsHandler.HandleRemoteRows(rows, execContext)
		if err != nil {
			return err
		}
	}
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

func (p *PushEngine) checkForRowsToForward() error {
	// If the node failed previously there may be un-forwarded rows in the forwarder table
	// If there are we need to forwarded them
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
		exist, err := p.ExistRowsInLocalTable(tableID, shardIDs)
		return !exist, err
	}, 5*time.Second, 100*time.Millisecond)
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
		return fmt.Errorf("there is %d source", len(p.sources))
	}
	if len(p.materializedViews) > 0 {
		return fmt.Errorf("there is %d materialized view", len(p.materializedViews))
	}
	return nil
}

func (p *PushEngine) GetScheduler(shardID uint64) (*sched.ShardScheduler, bool) {
	p.lock.RLock()
	defer p.lock.RUnlock()
	sched, ok := p.schedulers[shardID]
	return sched, ok
}

func (p *PushEngine) RemoveSource(sourceInfo *common.SourceInfo, deleteData bool) error {
	p.lock.Lock()
	defer p.lock.Unlock()

	src, ok := p.sources[sourceInfo.ID]
	if !ok {
		return fmt.Errorf("no such source %d", sourceInfo.ID)
	}
	if err := src.Stop(); err != nil {
		return err
	}

	delete(p.sources, sourceInfo.ID)
	delete(p.remoteConsumers, sourceInfo.ID)

	if deleteData {
		// Delete the committed offsets for the source
		startPrefix := common.AppendUint64ToBufferBE(nil, common.OffsetsTableID)
		startPrefix = common.KeyEncodeString(startPrefix, sourceInfo.SchemaName)
		startPrefix = common.KeyEncodeString(startPrefix, sourceInfo.Name)
		endPrefix := common.IncrementBytesBigEndian(startPrefix)

		if err := p.cluster.DeleteAllDataInRange(startPrefix, endPrefix); err != nil {
			return err
		}
		// Delete the table data
		return p.deleteAllDataForTable(sourceInfo.ID)
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
			source.RemoveConsumingExecutor(node)
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
	startPrefix := common.AppendUint64ToBufferBE(nil, tableID)
	endPrefix := common.AppendUint64ToBufferBE(nil, tableID+1)
	return p.cluster.DeleteAllDataInRange(startPrefix, endPrefix)
}

func (p *PushEngine) CreateSource(sourceInfo *common.SourceInfo) error {
	_, err := p.createSource(sourceInfo)
	return err
}

func (p *PushEngine) StartSource(sourceID uint64) error {
	src, err := p.GetSource(sourceID)
	if err != nil {
		return err
	}
	return src.Start() // Outside the lock
}

func (p *PushEngine) createSource(sourceInfo *common.SourceInfo) (*source.Source, error) {

	colTypes := sourceInfo.TableInfo.ColumnTypes

	tableExecutor := exec.NewTableExecutor(colTypes, sourceInfo.TableInfo, p.cluster)

	p.lock.Lock()
	defer p.lock.Unlock()

	src, err := source.NewSource(
		sourceInfo,
		tableExecutor,
		p.sharder,
		p.cluster,
		p.mover,
		p,
		p.cfg,
		p.queryExec,
	)
	if err != nil {
		return nil, err
	}

	rf := common.NewRowsFactory(colTypes)
	rc := &remoteConsumer{
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
