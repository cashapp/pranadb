package push

import (
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/squareup/pranadb/failinject"
	"github.com/squareup/pranadb/interruptor"
	"github.com/squareup/pranadb/parplan"
	"github.com/squareup/pranadb/protos/squareup/cash/pranadb/v1/notifications"
	"github.com/squareup/pranadb/push/util"
	"github.com/squareup/pranadb/remoting"
	"github.com/squareup/pranadb/tidb/planner"
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
	processBatchTimeHistogram metrics.Observer
	globalRateLimiter         ratelimit.Limiter
	failInject                failinject.Injector
	lagsBroadcastClient       remoting.Client
	lagsTimer                 *time.Timer
	shardLags                 sync.Map
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
	lagsBroadcastClient := remoting.NewClient(cfg.NotifListenAddresses...)
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
		lagsBroadcastClient:       lagsBroadcastClient,
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
	p.cluster.AddHealthcheckListener(p.lagsBroadcastClient.AvailabilityListener())
	if err := p.lagsBroadcastClient.Start(); err != nil {
		return err
	}
	p.broadcastLagsNoLock()
	p.started = true
	return nil
}

// Ready signals that the push engine is now ready to receive any incoming data
func (p *Engine) Ready() error {

	p.localShardsLock.Lock()
	defer p.localShardsLock.Unlock()

	// Now we can activate the schedulers
	for _, shardID := range p.localLeaderShards {
		shard := p.schedulers[shardID]
		shard.Start()
	}
	return nil
}

func (p *Engine) Stop() error {
	p.lock.Lock()
	defer p.lock.Unlock()
	if !p.started {
		return nil
	}
	if p.lagsTimer != nil {
		p.lagsTimer.Stop()
	}
	if err := p.lagsBroadcastClient.Stop(); err != nil {
		return err
	}
	//p.readyToReceive.Set(false)
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

func (p *Engine) CreateIndex(indexInfo *common.IndexInfo, fill bool, interruptor *interruptor.Interruptor) error {

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
		log.Println("Filling index")
		// And fill it with the data from the table - this creates the index
		if err := te.FillTo(indexExec, consumerName, indexInfo.ID, schedulers, p.failInject, interruptor); err != nil {
			return err
		}
	} else {
		// Just attach it directly
		te.AddConsumingNode(consumerName, indexExec)
	}
	return nil
}

func (p *Engine) UnattachIndex(indexInfo *common.IndexInfo) error {
	te, err := p.getTableExecutorForIndex(indexInfo)
	if err != nil {
		return err
	}
	consumerName := fmt.Sprintf("%s.%s", te.TableInfo.Name, indexInfo.Name)
	te.RemoveConsumingNode(consumerName)
	return nil
}

func (p *Engine) RemoveIndex(indexInfo *common.IndexInfo) error {
	if err := p.UnattachIndex(indexInfo); err != nil {
		return err
	}

	// Delete the table dataf
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
			return nil, errors.NewUnknownTableError(indexInfo.SchemaName, indexInfo.TableName)
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
	log.Printf("creating new scheduler for shard %d", shardID)
	p.lock.Lock()
	defer p.lock.Unlock()
	p.localShardsLock.Lock()
	defer p.localShardsLock.Unlock()
	sh := sched.NewShardScheduler(shardID, p)
	p.schedulers[shardID] = sh
	p.localLeaderShards = append(p.localLeaderShards, shardID)
	return &shardListener{
		shardID: shardID,
		p:       p,
		sched:   sh,
	}
}

func (s *shardListener) RemoteWriteOccurred(forwardRows []cluster.ForwardRow) {
	s.sched.AddRows(forwardRows)
}

type RawRow struct {
	ReceiverSequence uint64
	Row              []byte
}

func (p *Engine) HandleBatch(shardID uint64, rowsBatch []cluster.ForwardRow, first bool) (int64, error) {
	rawRows := make(map[uint64][]RawRow)

	if first {
		// For the first batch we actually load it directly from the receiver table - there may be pending data there
		// That was undelivered from the last time the server was started - we need to deliver everything in there first
		return p.loadReceivedRows(shardID)
	}

	receiveBatch := &receiveBatch{
		writeBatch: cluster.NewWriteBatch(shardID),
	}

	nr := len(rowsBatch)
	for _, row := range rowsBatch {
		consumerRows, ok := rawRows[row.RemoteConsumerID]
		if !ok {
			consumerRows = make([]RawRow, 0, nr)
		}
		consumerRows = append(consumerRows, RawRow{
			ReceiverSequence: row.ReceiverSequence,
			Row:              row.RowBytes,
		})
		rawRows[row.RemoteConsumerID] = consumerRows
		// TODO we can delete range instead of deleting one by one
		receiveBatch.writeBatch.AddDelete(row.KeyBytes)
	}

	receiveBatch.rawRows = rawRows

	if err := p.processReceiveBatch(receiveBatch); err != nil {
		return 0, err
	}
	return int64(rowsBatch[len(rowsBatch)-1].ReceiverSequence), nil
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
	writeBatch *cluster.WriteBatch
	rawRows    map[uint64][]RawRow
}

func (p *Engine) loadReceivedRows(receivingShardID uint64) (int64, error) {
	keyStartPrefix := table.EncodeTableKeyPrefix(common.ReceiverTableID, receivingShardID, 16)
	keyEndPrefix := table.EncodeTableKeyPrefix(common.ReceiverTableID+1, receivingShardID, 16)

	// Format of key is:
	// shard_id|receiver_table_id|receiver_sequence|remote_consumer_id|
	kvPairs, err := p.cluster.LocalScan(keyStartPrefix, keyEndPrefix, -1)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	if len(kvPairs) == 0 {
		return -1, nil
	}

	batch := &receiveBatch{
		writeBatch: cluster.NewWriteBatch(receivingShardID),
		rawRows:    make(map[uint64][]RawRow),
	}

	var receiverSequence uint64
	for _, kvPair := range kvPairs {
		receiverSequence, _ = common.ReadUint64FromBufferBE(kvPair.Key, 16)
		remoteConsumerID, _ := common.ReadUint64FromBufferBE(kvPair.Key, 24)
		rows := batch.rawRows[remoteConsumerID]
		rows = append(rows, RawRow{
			ReceiverSequence: receiverSequence,
			Row:              kvPair.Value,
		})
		batch.rawRows[remoteConsumerID] = rows
		batch.writeBatch.AddDelete(kvPair.Key)
	}

	if err := p.processReceiveBatch(batch); err != nil {
		return 0, err
	}
	log.Printf("shard %d processed %d pending rows", receivingShardID, len(kvPairs))
	return int64(receiverSequence), nil
}

func (p *Engine) processReceiveBatch(batch *receiveBatch) error {
	ctx := exec.NewExecutionContext(batch.writeBatch, -1)
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
			lpvb, _ := common.ReadUint32FromBufferLE(row.Row, 0)
			pi := -1
			if lpvb != 0 {
				prevBytes := row.Row[4 : 4+lpvb]
				if err := common.DecodeRow(prevBytes, remoteConsumer.ColTypes, rows); err != nil {
					return errors.WithStack(err)
				}
				pi = rc
				rc++
			}
			lcvb, _ := common.ReadUint32FromBufferLE(row.Row, int(4+lpvb))
			ci := -1
			if lcvb != 0 {
				currBytes := row.Row[8+lpvb:]
				if err := common.DecodeRow(currBytes, remoteConsumer.ColTypes, rows); err != nil {
					return errors.WithStack(err)
				}
				ci = rc
				rc++
			}
			entries[i] = exec.NewRowsEntry(pi, ci, int64(row.ReceiverSequence))
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

// WaitForProcessingToComplete is used in tests to wait for all rows have been processed when ingesting test data
func (p *Engine) WaitForProcessingToComplete() error {

	log.Println("waiting for schedulers")
	err := p.WaitForSchedulers()
	if err != nil {
		return errors.WithStack(err)
	}

	//// Wait for no rows in the receiver table
	log.Println("waiting for no rows in receiver")
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
		sched.WaitForProcessingToComplete(ch)
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
	}, 10*time.Second, 100*time.Millisecond)
	if !ok {
		log.Printf("Rows in table %d", tableID)
		p.dumpRowsInTable(tableID, shardIDs)
		return errors.Error("timed out waiting for condition")
	}
	return errors.WithStack(err)
}

func (p *Engine) dumpRowsInTable(tableID uint64, localShards []uint64) {
	for _, shardID := range localShards {
		startPrefix := table.EncodeTableKeyPrefix(tableID, shardID, 16)
		endPrefix := table.EncodeTableKeyPrefix(tableID+1, shardID, 16)
		kvPairs, err := p.cluster.LocalScan(startPrefix, endPrefix, 1)
		if err != nil {
			panic(err)
		}
		for _, pair := range kvPairs {
			sid, _ := common.ReadUint64FromBufferBE(pair.Key, 0)
			log.Printf("shardid%d key:%v value:%v", sid, pair.Key, pair.Value)
		}
	}
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

var initBatchSize = 10000
var initBatchSizeLock = sync.RWMutex{}

func getInitBatchSize() int {
	initBatchSizeLock.Lock()
	defer initBatchSizeLock.Unlock()
	return initBatchSize
}

func SetInitBatchSize(batchSize int) {
	initBatchSizeLock.Lock()
	defer initBatchSizeLock.Unlock()
	initBatchSize = batchSize
}

func (p *Engine) CreateSource(sourceInfo *common.SourceInfo, initTable *common.TableInfo) (*source.Source, error) {

	p.lock.Lock()
	defer p.lock.Unlock()

	ingestFilter := sourceInfo.OriginInfo.IngestFilter
	var ingestExpressions []*common.Expression
	if ingestFilter != "" {
		// To create the ingest filter we create a fake table in the meta-store with the same columns as the real source
		// then we create a push query plan from that for a query formed from the ingest filter.
		// We then extract the filter expressions from the select in that physical plan.
		// The filter expressions are then executed against the row when it's ingested.
		tmpID, err := p.cluster.GenerateClusterSequence("table")
		if err != nil {
			return nil, err
		}
		schemaName := fmt.Sprintf("tmp_schema_%d", tmpID)
		schema := p.meta.GetOrCreateSchema(schemaName)
		defer func() {
			p.meta.DeleteSchemaIfEmpty(schema)
		}()
		tabName := fmt.Sprintf("tmp_source_filter_%d", tmpID)
		tabInfo := &common.TableInfo{
			ID:             tmpID,
			SchemaName:     schemaName,
			Name:           tabName,
			PrimaryKeyCols: sourceInfo.PrimaryKeyCols,
			ColumnNames:    sourceInfo.ColumnNames,
			ColumnTypes:    sourceInfo.ColumnTypes,
		}
		tmpSourceInfo := &common.SourceInfo{
			TableInfo:  tabInfo,
			OriginInfo: nil,
		}
		if err := p.meta.RegisterSource(tmpSourceInfo); err != nil {
			return nil, err
		}
		defer func() {
			// Make sure we unregister the tmp source
			if err := p.meta.UnregisterSource(schemaName, tabName); err != nil {
				log.Errorf("failed to unregister tmp source %v", err)
			}
		}()
		pl := parplan.NewPlanner(schema)
		query := fmt.Sprintf("select * from %s where %s", tabName, ingestFilter)
		phys, _, _, err := pl.QueryToPlan(query, false, false)
		var sel *planner.PhysicalSelection
		if err == nil {
			var ok bool
			sel, ok = phys.(*planner.PhysicalSelection)
			if !ok {
				log.Errorf(" ingest filter %s on %s.%s gave invalid physical plan %v", ingestFilter,
					sourceInfo.SchemaName, sourceInfo.Name, phys)
			}
		}
		if err != nil || sel == nil {
			return nil, errors.NewPranaErrorf(errors.InvalidStatement, "invalid ingest filter \"%s\"", ingestFilter)
		}
		ingestExpressions = make([]*common.Expression, len(sel.Conditions))
		for i, expr := range sel.Conditions {
			ingestExpressions[i] = common.NewExpression(expr, sel.SCtx())
		}
	}

	tableExecutor := exec.NewTableExecutor(sourceInfo.TableInfo, p.cluster, p)

	src, err := source.NewSource(
		sourceInfo,
		tableExecutor,
		ingestExpressions,
		p.sharder,
		p.cluster,
		p.cfg,
		p.queryExec,
		p.protoRegistry,
		p,
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

func (p *Engine) LoadInitialStateForTable(shardIDs []uint64, initTableID uint64, targetTableID uint64,
	inter *interruptor.Interruptor) error {
	log.Debugf("loading initial state for table %d from %d", targetTableID, initTableID)
	batchSize := getInitBatchSize()
	delayer := interruptor.GetInterruptManager()
	for _, shardID := range shardIDs {
		scanStart := table.EncodeTableKeyPrefix(initTableID, shardID, 16)
		scanEnd := table.EncodeTableKeyPrefix(initTableID+1, shardID, 16)
		newKeyPrefix := table.EncodeTableKeyPrefix(targetTableID, shardID, 64)
		skipFirst := false
		for {
			if delayer.MaybeInterrupt("initial_state", inter) {
				return errors.NewPranaErrorf(errors.DdlCancelled, "Loading initial state for table cancelled")
			}
			pairs, err := p.cluster.LocalScan(scanStart, scanEnd, batchSize)
			if err != nil {
				return err
			}
			wb := cluster.NewWriteBatch(shardID)
			for i, kv := range pairs {
				if skipFirst && i == 0 {
					continue
				}
				key := make([]byte, 0, len(kv.Key))
				key = append(key, newKeyPrefix...)
				key = append(key, kv.Key[16:]...)
				wb.AddPut(key, kv.Value)
			}
			if err := p.cluster.WriteBatchLocally(wb); err != nil {
				return err
			}
			if len(pairs) < batchSize {
				break
			}
			scanStart = pairs[len(pairs)-1].Key
			skipFirst = true
			util.MaybeThrottleIfLagging(p.cluster.GetAllShardIDs(), p, 5*time.Second)
		}
	}
	if err := p.cluster.SyncStore(); err != nil {
		return err
	}
	log.Debugf("loaded initial state for table %d from %d", targetTableID, initTableID)
	return nil
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

func (p *Engine) getLagsMessage() *notifications.LagsMessage {
	p.localShardsLock.Lock()
	defer p.localShardsLock.Unlock()

	lags := make([]*notifications.LagEntry, len(p.localLeaderShards))
	now := time.Now()
	for i, shardID := range p.localLeaderShards {
		sh := p.schedulers[shardID]
		lag := sh.GetLag(now)
		lagEntry := &notifications.LagEntry{
			ShardId: int64(shardID),
			Lag:     int64(lag),
		}
		lags[i] = lagEntry
	}
	return &notifications.LagsMessage{Lags: lags}
}

func (p *Engine) broadcastLags() {
	p.lock.Lock()
	defer p.lock.Unlock()
	if !p.started {
		return
	}
	p.broadcastLagsNoLock()
}

func (p *Engine) broadcastLagsNoLock() {
	p.lagsTimer = time.AfterFunc(1*time.Second, func() {
		msg := p.getLagsMessage()
		if err := p.lagsBroadcastClient.BroadcastOneway(msg); err != nil {
			log.Errorf("failed to broadcast lags %+v", err)
		} else {
			p.broadcastLags()
		}
	})
}

func (p *Engine) HandleMessage(notification remoting.ClusterMessage) (remoting.ClusterMessage, error) {
	lags, ok := notification.(*notifications.LagsMessage)
	if !ok {
		panic("not a LagsMessage")
	}
	for _, lagEntry := range lags.Lags {
		shardID := uint64(lagEntry.ShardId)
		lag := time.Duration(lagEntry.Lag)
		p.shardLags.Store(shardID, lag)
	}
	return nil, nil
}

func (p *Engine) GetLag(shardID uint64) time.Duration {
	l, ok := p.shardLags.Load(shardID)
	if !ok {
		return time.Duration(0)
	}
	lag, ok := l.(time.Duration)
	if !ok {
		panic("not a time.Duration")
	}
	return lag
}
