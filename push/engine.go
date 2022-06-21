package push

import (
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/squareup/pranadb/failinject"
	"github.com/squareup/pranadb/push/util"
	"go.uber.org/ratelimit"
	"math/rand"
	"sync"
	"time"

	"github.com/squareup/pranadb/metrics"

	"github.com/squareup/pranadb/errors"

	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/conf"
	"github.com/squareup/pranadb/protolib"
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

type Engine struct {
	lock                      sync.RWMutex
	localShardsLock           sync.RWMutex
	started                   bool
	schedulers                map[uint64]*sched.ShardScheduler
	sources                   map[uint64]*source.Source
	materializedViews         map[uint64]*MaterializedView
	remoteConsumers           sync.Map
	localLeaderShards         []uint64
	cluster                   cluster.Cluster
	sharder                   *sharder.Sharder
	meta                      *meta.Controller
	rnd                       *rand.Rand
	cfg                       *conf.Config
	queryExec                 common.SimpleQueryExec
	protoRegistry             protolib.Resolver
	readyToReceive            common.AtomicBool
	processBatchTimeHistogram metrics.Observer
	globalRateLimiter         ratelimit.Limiter
	failInject                failinject.Injector
}

var (
	processBatchVec = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "pranadb_process_batch_time_nanos",
		Help: "histogram measuring time to process batches of rows in the push engine in nanoseconds",
	}, []string{"node_id"})
)

// RemoteConsumer is a wrapper for something that consumes rows that have arrived remotely from other shards
// e.g. a source or an aggregator
type RemoteConsumer struct {
	RowsFactory *common.RowsFactory
	ColTypes    []common.ColumnType
	RowsHandler remoteRowsHandler
}

type shardListener struct {
	shardID uint64
	p       *Engine
	sched   *sched.ShardScheduler
}

type remoteRowsHandler interface {
	HandleRemoteRows(rowsBatch exec.RowsBatch, ctx *exec.ExecutionContext) error
}

func NewPushEngine(cluster cluster.Cluster, sharder *sharder.Sharder, meta *meta.Controller, cfg *conf.Config,
	queryExec common.SimpleQueryExec, registry protolib.Resolver, failInject failinject.Injector) *Engine {
	// We limit the ingest rate of the source to this value - this prevents the node getting overloaded which can result
	// in unstable behaviour
	var rl ratelimit.Limiter
	if cfg.GlobalIngestLimitRowsPerSec != -1 {
		rl = ratelimit.New(cfg.GlobalIngestLimitRowsPerSec)
	} else {
		rl = nil
	}
	engine := Engine{
		cluster:                   cluster,
		sharder:                   sharder,
		meta:                      meta,
		rnd:                       rand.New(rand.NewSource(time.Now().UTC().UnixNano())),
		cfg:                       cfg,
		queryExec:                 queryExec,
		protoRegistry:             registry,
		processBatchTimeHistogram: processBatchVec.WithLabelValues(fmt.Sprintf("node-%d", cluster.GetNodeID())),
		globalRateLimiter:         rl,
		failInject:                failInject,
	}
	engine.createMaps()
	return &engine
}

func (p *Engine) Start() error {
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.started {
		return nil
	}

	p.started = true
	return nil
}

// Ready signals that the push engine is now ready to receive any incoming data
func (p *Engine) Ready() error {
	p.readyToReceive.Set(true)
	return p.checkForPendingData()
}

func (p *Engine) Stop() error {
	p.lock.Lock()
	defer p.lock.Unlock()
	if !p.started {
		return nil
	}
	p.readyToReceive.Set(false)
	for _, src := range p.sources {
		if err := src.Stop(); err != nil {
			return errors.WithStack(err)
		}
	}
	for _, sh := range p.schedulers {
		sh.Stop()
	}
	p.createMaps() // Clear the internal state
	p.started = false
	return nil
}

func (p *Engine) GetSource(sourceID uint64) (*source.Source, error) {
	p.lock.RLock()
	defer p.lock.RUnlock()
	source, ok := p.sources[sourceID]
	if !ok {
		return nil, errors.Error("no such source")
	}
	return source, nil
}

func (p *Engine) RemoveSource(sourceInfo *common.SourceInfo) (*source.Source, error) {
	p.lock.Lock()
	defer p.lock.Unlock()

	src, ok := p.sources[sourceInfo.ID]
	if !ok {
		return nil, errors.Errorf("no such source %d", sourceInfo.ID)
	}
	if src.IsRunning() {
		return nil, errors.Error("source is running")
	}

	delete(p.sources, sourceInfo.ID)
	p.remoteConsumers.Delete(sourceInfo.ID)

	return src, nil
}

func (p *Engine) RegisterRemoteConsumer(id uint64, rc *RemoteConsumer) error {
	p.lock.Lock()
	defer p.lock.Unlock()

	if _, ok := p.remoteConsumers.Load(id); ok {
		return errors.Errorf("remote consumer with id %d already registered", id)
	}
	p.remoteConsumers.Store(id, rc)
	return nil
}

func (p *Engine) UnregisterRemoteConsumer(id uint64) error {
	p.lock.Lock()
	defer p.lock.Unlock()

	_, ok := p.remoteConsumers.Load(id)
	if !ok {
		return errors.Errorf("remote consumer with id %d not registered", id)
	}
	p.remoteConsumers.Delete(id)
	return nil
}

func (p *Engine) GetMaterializedView(mvID uint64) (*MaterializedView, error) {
	p.lock.RLock()
	defer p.lock.RUnlock()
	mv, ok := p.materializedViews[mvID]
	if !ok {
		return nil, errors.Errorf("no such materialized view %d", mvID)
	}
	return mv, nil
}

func (p *Engine) RemoveMV(mvID uint64) error {
	p.lock.Lock()
	defer p.lock.Unlock()
	_, ok := p.materializedViews[mvID]
	if !ok {
		return errors.Errorf("cannot find materialized view with id %d", mvID)
	}
	delete(p.materializedViews, mvID)
	return nil
}

func (p *Engine) RegisterMV(mv *MaterializedView) error {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.materializedViews[mv.Info.TableInfo.ID] = mv
	return nil
}

func (p *Engine) CreateIndex(indexInfo *common.IndexInfo, fill bool) error {

	schedulers, err := p.GetLocalLeaderSchedulers()
	if err != nil {
		return errors.WithStack(err)
	}

	te, err := p.getTableExecutorForIndex(indexInfo)
	if err != nil {
		return err
	}

	// Create an index executor
	indexExec := exec.NewIndexExecutor(te.TableInfo, indexInfo, p.cluster)

	consumerName := fmt.Sprintf("%s.%s", te.TableInfo.Name, indexInfo.Name)
	if fill {
		// And fill it with the data from the table - this creates the index
		if err := te.FillTo(indexExec, consumerName, indexInfo.ID, schedulers, p.failInject); err != nil {
			return err
		}
	} else {
		// Just attach it directly
		te.AddConsumingNode(consumerName, indexExec)
	}
	return nil
}

func (p *Engine) RemoveIndex(indexInfo *common.IndexInfo) error {
	te, err := p.getTableExecutorForIndex(indexInfo)
	if err != nil {
		return err
	}
	consumerName := fmt.Sprintf("%s.%s", te.TableInfo.Name, indexInfo.Name)
	te.RemoveConsumingNode(consumerName)

	// Delete the table data
	tableStartPrefix := common.AppendUint64ToBufferBE(nil, indexInfo.ID)
	tableEndPrefix := common.AppendUint64ToBufferBE(nil, indexInfo.ID+1)
	return p.cluster.DeleteAllDataInRangeForAllShardsLocally(tableStartPrefix, tableEndPrefix)
}

func (p *Engine) getTableExecutorForIndex(indexInfo *common.IndexInfo) (*exec.TableExecutor, error) {
	// Find the table executor for the source / mv that we are creating the index on
	var te *exec.TableExecutor
	srcInfo, ok := p.meta.GetSource(indexInfo.SchemaName, indexInfo.TableName)
	if !ok {
		mvInfo, ok := p.meta.GetMaterializedView(indexInfo.SchemaName, indexInfo.TableName)
		if !ok {
			return nil, errors.NewUnknownSourceOrMaterializedViewError(indexInfo.SchemaName, indexInfo.TableName)
		}
		mv, err := p.GetMaterializedView(mvInfo.ID)
		if err != nil {
			return nil, err
		}
		te = mv.TableExecutor()
	} else {
		src, err := p.GetSource(srcInfo.ID)
		if err != nil {
			return nil, err
		}
		te = src.TableExecutor()
	}
	return te, nil
}

func (p *Engine) CreateShardListener(shardID uint64) cluster.ShardListener {
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
	s.p.MaybeHandleRemoteBatch(s.sched)
}

func (p *Engine) MaybeHandleRemoteBatch(scheduler *sched.ShardScheduler) {
	scheduler.ScheduleActionFireAndForget(func() error {
		start := time.Now()
		if err := p.HandleReceivedRows(scheduler.ShardID()); err != nil {
			return errors.WithStack(err)
		}
		durNanos := time.Now().Sub(start).Nanoseconds()
		p.processBatchTimeHistogram.Observe(float64(durNanos))
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

func (p *Engine) removeScheduler(shardID uint64) {
	p.lock.Lock()
	defer p.lock.Unlock()
	delete(p.schedulers, shardID)
}

type receiveBatch struct {
	batchSequence uint32
	writeBatch    *cluster.WriteBatch
	rawRows       map[uint64][][]byte
}

// HandleReceivedRows - load batches of rows from the Receiver table and process them
func (p *Engine) HandleReceivedRows(receivingShardID uint64) error {
	keyStartPrefix := table.EncodeTableKeyPrefix(common.ReceiverTableID, receivingShardID, 16)
	keyEndPrefix := table.EncodeTableKeyPrefix(common.ReceiverTableID+1, receivingShardID, 16)

	kvPairs, err := p.cluster.LocalScan(keyStartPrefix, keyEndPrefix, -1)
	if err != nil {
		return errors.WithStack(err)
	}

	var receiveBatches []*receiveBatch
	var currBatch *receiveBatch
	var prevBatchSequence uint32

	// Format of key is:
	// shard_id|receiver_table_id|batch_sequence|receiver_sequence|remote_consumer_id|

	// We iterate through the pairs and create a receiveBatch for each value of batchSequence
	for _, kvPair := range kvPairs {
		batchSequence, _ := common.ReadUint32FromBufferBE(kvPair.Key, 16)
		if currBatch == nil || batchSequence != prevBatchSequence {
			currBatch = &receiveBatch{
				batchSequence: batchSequence,
				writeBatch:    cluster.NewWriteBatch(receivingShardID),
				rawRows:       make(map[uint64][][]byte),
			}
			receiveBatches = append(receiveBatches, currBatch)
			prevBatchSequence = batchSequence
		}

		remoteConsumerID, _ := common.ReadUint64FromBufferBE(kvPair.Key, 28)
		rows, ok := currBatch.rawRows[remoteConsumerID]
		if !ok {
			rows = make([][]byte, 0)
		}
		rows = append(rows, kvPair.Value)
		currBatch.rawRows[remoteConsumerID] = rows
		currBatch.writeBatch.AddDelete(kvPair.Key)
	}

	for _, receiveBatch := range receiveBatches {
		if err := p.processReceiveBatch(receiveBatch); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func (p *Engine) processReceiveBatch(batch *receiveBatch) error {
	ctx := exec.NewExecutionContext(batch.writeBatch, true)
	ctx.BatchSequence = batch.batchSequence
	for entityID, rawRows := range batch.rawRows {
		rcVal, ok := p.remoteConsumers.Load(entityID)
		if !ok {
			// Does the entity exist in storage?
			rows, err := p.queryExec.ExecuteQuery("sys", fmt.Sprintf("select id from tables where id=%d", entityID))
			if err != nil {
				return errors.WithStack(err)
			}
			if rows.RowCount() == 1 {
				// The entity is in storage but not deployed - this might happen if a node joined when a create source/mv
				// was in progress so did not get the notifications but did see it in storage - in this case
				// we periodically scan sys.tables to check for any non registered entities TODO
				return errors.Errorf("entity with id %d not registered", entityID)
			}
			// The entity does not exist in storage - it must correspond to a dropped entity - we can ignore the row
			// and it will get deleted from the receiver table
			log.Warnf("Received rows - Entity with id %d is not registered and does not exist in storage. Will be ignored as likely corresponds to a dropped source or materialized view.", entityID)
			continue
		}

		remoteConsumer := rcVal.(*RemoteConsumer) //nolint:forcetypeassert
		rows := remoteConsumer.RowsFactory.NewRows(len(rawRows))
		entries := make([]exec.RowsEntry, len(rawRows))
		rc := 0
		for i, row := range rawRows {
			lpvb, _ := common.ReadUint32FromBufferLE(row, 0)
			pi := -1
			if lpvb != 0 {
				prevBytes := row[4 : 4+lpvb]
				if err := common.DecodeRow(prevBytes, remoteConsumer.ColTypes, rows); err != nil {
					return errors.WithStack(err)
				}
				pi = rc
				rc++
			}
			lcvb, _ := common.ReadUint32FromBufferLE(row, int(4+lpvb))
			ci := -1
			if lcvb != 0 {
				currBytes := row[8+lpvb:]
				if err := common.DecodeRow(currBytes, remoteConsumer.ColTypes, rows); err != nil {
					return errors.WithStack(err)
				}
				ci = rc
				rc++
			}
			entries[i] = exec.NewRowsEntry(pi, ci)
		}
		rowsBatch := exec.NewRowsBatch(rows, entries)
		if err := remoteConsumer.RowsHandler.HandleRemoteRows(rowsBatch, ctx); err != nil {
			return errors.WithStack(err)
		}
	}
	// Now send any remote forward batches - e.g. from aggregations
	if err := util.SendForwardBatches(ctx.RemoteBatches, p.cluster); err != nil {
		return errors.WithStack(err)
	}
	// Maybe inject an error after we have forwarded remote batches but before we have committed local batch
	if err := p.failInject.GetFailpoint("process_batch_before_local_commit").CheckFail(); err != nil {
		return err
	}
	if err := p.cluster.WriteBatch(batch.writeBatch); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (p *Engine) checkForPendingData() error {
	log.Debug("Checking for data in receiver table")
	// If the node failed previously or received messages it was unable to handle as it was starting up then
	// there could be rows in the receiver table
	// we check and process these, if there are any
	for _, scheduler := range p.schedulers {
		p.MaybeHandleRemoteBatch(scheduler)
	}
	return nil
}

// WaitForProcessingToComplete is used in tests to wait for all rows have been processed when ingesting test data
func (p *Engine) WaitForProcessingToComplete() error {

	err := p.WaitForSchedulers()
	if err != nil {
		return errors.WithStack(err)
	}

	// Wait for no rows in the receiver table
	err = p.waitForNoRowsInTable(common.ReceiverTableID)
	if err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func (p *Engine) WaitForSchedulers() error {
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
			return errors.Error("chan was closed")
		}
	}
	return nil
}

func (p *Engine) waitForNoRowsInTable(tableID uint64) error {
	shardIDs := p.cluster.GetLocalShardIDs()
	ok, err := commontest.WaitUntilWithError(func() (bool, error) {
		exist, err := p.ExistRowsInLocalTable(tableID, shardIDs)
		return !exist, errors.WithStack(err)
	}, 30*time.Second, 100*time.Millisecond)
	if !ok {
		return errors.Error("timed out waiting for condition")
	}
	return errors.WithStack(err)
}

func (p *Engine) ExistRowsInLocalTable(tableID uint64, localShards []uint64) (bool, error) {
	for _, shardID := range localShards {
		startPrefix := table.EncodeTableKeyPrefix(tableID, shardID, 16)
		endPrefix := table.EncodeTableKeyPrefix(tableID+1, shardID, 16)
		kvPairs, err := p.cluster.LocalScan(startPrefix, endPrefix, 1)
		if err != nil {
			return false, errors.WithStack(err)
		}
		if kvPairs != nil {
			return true, nil
		}
	}
	return false, nil
}

func (p *Engine) VerifyNoSourcesOrMVs() error {
	if len(p.sources) > 0 {
		return errors.Errorf("there is %d source", len(p.sources))
	}
	if len(p.materializedViews) > 0 {
		return errors.Errorf("there is %d materialized view", len(p.materializedViews))
	}
	return nil
}

func (p *Engine) GetScheduler(shardID uint64) (*sched.ShardScheduler, bool) {
	p.lock.RLock()
	defer p.lock.RUnlock()
	sched, ok := p.schedulers[shardID]
	return sched, ok
}

func (p *Engine) CreateSource(sourceInfo *common.SourceInfo) (*source.Source, error) {

	p.lock.Lock()
	defer p.lock.Unlock()

	tableExecutor := exec.NewTableExecutor(sourceInfo.TableInfo, p.cluster)

	src, err := source.NewSource(
		sourceInfo,
		tableExecutor,
		p.sharder,
		p.cluster,
		p.cfg,
		p.queryExec,
		p.protoRegistry,
		p,
	)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	colTypes := sourceInfo.TableInfo.ColumnTypes
	rf := common.NewRowsFactory(colTypes)
	rc := &RemoteConsumer{
		RowsFactory: rf,
		ColTypes:    colTypes,
		RowsHandler: src.TableExecutor(),
	}
	p.remoteConsumers.Store(sourceInfo.TableInfo.ID, rc)
	p.sources[sourceInfo.TableInfo.ID] = src
	return src, nil
}

func (p *Engine) createMaps() {
	p.remoteConsumers = sync.Map{}
	p.sources = make(map[uint64]*source.Source)
	p.materializedViews = make(map[uint64]*MaterializedView)
	p.schedulers = make(map[uint64]*sched.ShardScheduler)
}

func (p *Engine) GetLocalLeaderSchedulers() (map[uint64]*sched.ShardScheduler, error) {
	p.lock.RLock()
	defer p.lock.RUnlock()
	schedulers := make(map[uint64]*sched.ShardScheduler, len(p.localLeaderShards))
	for _, lls := range p.localLeaderShards {
		sched, ok := p.schedulers[lls]
		if !ok {
			return nil, errors.Errorf("no scheduler for local leader shard %d", lls)
		}
		schedulers[lls] = sched
	}
	return schedulers, nil
}

func (p *Engine) IsEmpty() bool {
	p.lock.Lock()
	defer p.lock.Unlock()
	numRecs := 0
	p.remoteConsumers.Range(func(key, value interface{}) bool {
		numRecs++
		return true
	})
	return len(p.sources) == 0 && len(p.materializedViews) == 0 && numRecs == 0
}

func (p *Engine) Limit() {
	if p.globalRateLimiter != nil {
		p.globalRateLimiter.Take()
	}
}
