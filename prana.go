package pranadb

import (
	"fmt"
	"github.com/pingcap/tidb/infoschema"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/exec"
	planner2 "github.com/squareup/pranadb/parplan"
	"github.com/squareup/pranadb/storage"
	"log"
	"sync"
)

func NewPranaNode(storage storage.Storage, nodeID int) *PranaNode {
	pranaNode := PranaNode{
		nodeID:          nodeID,
		storage:         storage,
		planner:         planner2.NewPlanner(),
		schemas:         make(map[string]*Schema),
		remoteConsumers: make(map[uint64]*remoteConsumer),
		schedulers:      make(map[uint64]*ShardScheduler),
	}
	pranaNode.mover = NewMover(storage, &pranaNode, &pranaNode)
	return &pranaNode
}

type PranaNode struct {
	lock            sync.RWMutex
	nodeID          int
	storage         storage.Storage
	schemas         map[string]*Schema
	planner         planner2.Planner
	mover           *Mover
	started         bool
	remoteConsumers map[uint64]*remoteConsumer
	schedulers      map[uint64]*ShardScheduler
	allShards       []uint64
}

// TODO does this need to be exported?
type Schema struct {
	Name    string
	Mvs     map[string]*MaterializedView
	Sources map[string]*Source
	Sinks   map[string]*Sink
}

type Sharder interface {
	CalculateShard(key []byte) (uint64, error)
}

type RemoteRowsHandler interface {
	HandleRows(rows *common.PushRows, ctx *exec.ExecutionContext) error
}

// remoteConsumer is a wrapper for something that consumes rows that have arrived remotely from other shards
// e.g. a source or an aggregator
type remoteConsumer struct {
	rowsFactory *common.RowsFactory
	colTypes    []common.ColumnType
	rowsHandler RemoteRowsHandler
}

func (p *PranaNode) Start() error {
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.started {
		return nil
	}

	err := p.loadSchemas()
	if err != nil {
		return err
	}

	err = p.loadAndStartSchedulers()
	if err != nil {
		return err
	}

	p.storage.SetRemoteWriteHandler(p)

	return nil
}

// Load the materialized views, sources and sinks from storage
// Load up their associated dags and wire them in
func (p *PranaNode) loadSchemas() error {
	// TODO
	return nil
}

// Load and start a scheduler for every shard for which we are a leader
// These are used to deliver any incoming rows for that shard
func (p *PranaNode) loadAndStartSchedulers() error {
	nodeInfo, err := p.storage.GetNodeInfo(p.nodeID)
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

func (p *PranaNode) loadAllShards() error {
	clusterInfo, err := p.storage.GetClusterInfo()
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

func (p *PranaNode) Stop() {
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

func (p *PranaNode) CreateSource(schemaName string, name string, columnNames []string, columnTypes []common.ColumnType, primaryKeyColumns []int, topicInfo *TopicInfo) error {
	p.lock.Lock()
	defer p.lock.Unlock()

	schema := p.getOrCreateSchema(schemaName)
	err := p.existsMvOrSource(schema, name)
	if err != nil {
		return err
	}
	tableID, err := p.genTableID()
	if err != nil {
		return err
	}
	source, err := NewSource(name, schemaName, tableID, columnNames, columnTypes, primaryKeyColumns, topicInfo, p.storage)
	if err != nil {
		return err
	}
	schema.Sources[name] = source
	rf, err := common.NewRowsFactory(columnTypes)
	if err != nil {
		return err
	}
	rc := &remoteConsumer{
		rowsFactory: rf,
		colTypes:    columnTypes,
		rowsHandler: source.TableExecutor,
	}
	p.remoteConsumers[tableID] = rc
	return nil
}

func (p *PranaNode) CreateMaterializedView(schemaName string, name string, query string) error {
	p.lock.Lock()
	defer p.lock.Unlock()

	schema := p.getOrCreateSchema(schemaName)
	err := p.existsMvOrSource(schema, name)
	if err != nil {
		return err
	}
	is, err := p.toInfoSchema()
	if err != nil {
		return err
	}
	tableID, err := p.genTableID()
	if err != nil {
		return err
	}
	mv, err := NewMaterializedView(name, query, tableID, schema, is, p.storage, p.planner, p.remoteConsumers)
	if err != nil {
		return err
	}
	schema.Mvs[name] = mv
	return nil
}

func (p *PranaNode) CreateSink(schemaName string, name string, materializedViewName string, topicInfo TopicInfo) error {
	panic("implement me")
}

func (p *PranaNode) DropSource(schemaName string, name string) error {
	panic("implement me")
}

func (p *PranaNode) DropMaterializedView(schemaName string, name string) error {
	panic("implement me")
}

func (p *PranaNode) DropSink(schemaName string, name string) error {
	panic("implement me")
}

func (p *PranaNode) CreatePushQuery(sql string) error {
	panic("implement me")
}

func (p *PranaNode) getOrCreateSchema(schemaName string) *Schema {
	schema, ok := p.schemas[schemaName]
	if !ok {
		schema = p.newSchema(schemaName)
		p.schemas[schemaName] = schema
	}
	return schema
}

func (p *PranaNode) existsMvOrSource(schema *Schema, name string) error {
	_, ok := schema.Mvs[name]
	if ok {
		return fmt.Errorf("materialized view with Name %s already exists in Schema %s", name, schema.Name)
	}
	_, ok = schema.Sources[name]
	if ok {
		return fmt.Errorf("source with Name %s already exists in Schema %s", name, schema.Name)
	}
	return nil
}

func (p *PranaNode) toInfoSchema() (infoschema.InfoSchema, error) {
	var schemaInfos []*common.SchemaInfo
	for _, schema := range p.schemas {
		tableInfos := make(map[string]*common.TableInfo)
		for mvName, mv := range schema.Mvs {
			tableInfos[mvName] = mv.Table.Info()
		}
		for sourceName, source := range schema.Sources {
			tableInfos[sourceName] = source.Table.Info()
		}
		schemaInfo := &common.SchemaInfo{
			SchemaName:  schema.Name,
			TablesInfos: tableInfos,
		}
		schemaInfos = append(schemaInfos, schemaInfo)
	}
	return planner2.NewPranaInfoSchema(schemaInfos)
}

func (p *PranaNode) getMaterializedView(schemaName string, name string) (mv *MaterializedView, ok bool) {
	schema, ok := p.schemas[schemaName]
	if !ok {
		return nil, false
	}
	mv, ok = schema.Mvs[name]
	return mv, ok
}

func (p *PranaNode) getSource(schemaName string, name string) (source *Source, ok bool) {
	schema, ok := p.schemas[schemaName]
	if !ok {
		return nil, false
	}
	source, ok = schema.Sources[name]
	return source, ok
}

// TODO does this need to be globally unique?
func (p *PranaNode) genTableID() (uint64, error) {
	return p.storage.GenerateTableID()
}

func (p *PranaNode) newSchema(name string) *Schema {
	return &Schema{
		Name:    name,
		Mvs:     make(map[string]*MaterializedView),
		Sources: make(map[string]*Source),
		Sinks:   make(map[string]*Sink),
	}
}

// HandleRemoteRows handles rows forwarded from other shards
func (p *PranaNode) HandleRemoteRows(entityValues map[uint64][][]byte, batch *storage.WriteBatch) error {

	// TODO use copy on write and atomic references to entities to avoid read lock
	p.lock.RLock()
	defer p.lock.RUnlock()

	for entityID, rawRows := range entityValues {
		rc, ok := p.remoteConsumers[entityID]
		if !ok {
			return fmt.Errorf("entity with id %d not registered", entityID)
		}
		rows := rc.rowsFactory.NewRows(len(rawRows))
		for _, row := range rawRows {
			err := common.DecodeRow(row, rc.colTypes, rows)
			if err != nil {
				return err
			}
		}
		execContext := &exec.ExecutionContext{
			WriteBatch: batch,
			Forwarder:  p.mover,
		}
		err := rc.rowsHandler.HandleRows(rows, execContext)
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *PranaNode) RemoteWriteOccurred(shardID uint64) {
	err := p.remoteBatchArrived(shardID)
	if err != nil {
		log.Println(err)
	}
}

// Triggered when some data is written into the forward queue of a shard for which this node
// is a leader. In this case we want to deliver that remote batch to any interested parties
func (p *PranaNode) remoteBatchArrived(shardID uint64) error {
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

func (p *PranaNode) CalculateShard(key []byte) (uint64, error) {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return ComputeShard(key, p.allShards), nil
}
