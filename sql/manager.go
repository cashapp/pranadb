package sql

import (
	"context"
	"fmt"
	"github.com/pingcap/tidb/infoschema"
	"github.com/squareup/pranadb/storage"
	"sync"
)

type Manager interface {
	CreateSource(schemaName string, name string, columnNames []string, columnTypes []ColumnType, primaryKeyCols []int, partitions int, topicInfo *TopicInfo) error

	CreateMaterializedView(schemaName string, name string, query string, partitions int) error

	CreateSink(schemaName string, name string, materializedViewName string, topicInfo TopicInfo) error

	DropSource(schemaName string, name string) error

	DropMaterializedView(schemaName string, name string) error

	DropSink(schemaName string, name string) error

	CreatePushQuery(sql string) error

	ToInfoSchema() (infoschema.InfoSchema, error)
}

type Encoding int

const (
	EncodingJSON Encoding = iota + 1
	EncodingProtobuf
	EncodingRaw
)

type TopicInfo struct {
	brokerName string
	topicName  string
	keyFormat  Encoding
	properties map[string]interface{}
}

func NewManager(storage storage.Storage) Manager {
	return &manager{
		storage: storage,
		parser:  NewParser(),
		planner: NewPlanner(),
		schemas: make(map[string]*schema),
	}
}

type manager struct {
	lock            sync.RWMutex
	tableIDSequence uint64
	storage         storage.Storage
	schemas         map[string]*schema
	parser          Parser
	planner         Planner
}

type schema struct {
	name    string
	mvs     map[string]*MaterializedView
	sources map[string]*Source
	sinks   map[string]*Sink
}

func (m *manager) newSchema(name string) *schema {
	return &schema{
		name:    name,
		mvs:     make(map[string]*MaterializedView),
		sources: make(map[string]*Source),
		sinks:   make(map[string]*Sink),
	}
}

func (m *manager) CreateSource(schemaName string, name string, columnNames []string, columnTypes []ColumnType, primaryKeyColumns []int, partitions int, topicInfo *TopicInfo) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	schema := m.getOrCreateSchema(schemaName)
	err := m.existsMvOrSource(schema, name)
	if err != nil {
		return err
	}

	id := m.tableIDSequence
	m.tableIDSequence++

	tableInfo := TableInfo{
		ID:             id,
		TableName:      name,
		ColumnNames:    columnNames,
		ColumnTypes:    columnTypes,
		PrimaryKeyCols: primaryKeyColumns,
		Partitions:     partitions,
	}

	sourceTable, err := NewTable(m.storage, &tableInfo)
	if err != nil {
		return err
	}

	tableExecutor, err := NewTableExecutor(columnTypes, sourceTable, m.storage)
	if err != nil {
		return err
	}

	source := Source{
		SchemaName:    schemaName,
		Name:          name,
		Table:         sourceTable,
		TopicInfo:     topicInfo,
		TableExecutor: tableExecutor,
	}

	schema.sources[name] = &source

	return nil
}

func (m *manager) CreateMaterializedView(schemaName string, name string, query string, partitions int) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	schema := m.getOrCreateSchema(schemaName)
	err := m.existsMvOrSource(schema, name)
	if err != nil {
		return err
	}

	is, err := m.toInfoSchema()
	if err != nil {
		return err
	}

	dag, err := m.buildPushQueryExecution(schema, is, query)
	if err != nil {
		return err
	}

	id := m.tableIDSequence
	m.tableIDSequence++

	tableInfo := TableInfo{
		ID:             id,
		TableName:      name,
		ColumnNames:    dag.ColNames(),
		ColumnTypes:    dag.ColTypes(),
		PrimaryKeyCols: dag.KeyCols(),
		Partitions:     partitions,
	}

	mvTable, err := NewTable(m.storage, &tableInfo)
	if err != nil {
		return err
	}

	tableNode, err := NewTableExecutor(dag.ColTypes(), mvTable, m.storage)
	if err != nil {
		return err
	}

	mv := MaterializedView{
		SchemaName:    schemaName,
		Name:          name,
		Query:         query,
		Table:         mvTable,
		TableExecutor: tableNode,
		store:         m.storage,
	}

	ConnectNodes([]PushExecutorNode{dag}, tableNode)

	schema.mvs[name] = &mv

	return nil
}

func (m manager) CreateSink(schemaName string, name string, materializedViewName string, topicInfo TopicInfo) error {
	panic("implement me")
}

func (m manager) DropSource(schemaName string, name string) error {
	panic("implement me")
}

func (m manager) DropMaterializedView(schemaName string, name string) error {
	panic("implement me")
}

func (m manager) DropSink(schemaName string, name string) error {
	panic("implement me")
}

func (m manager) CreatePushQuery(sql string) error {
	panic("implement me")
}

func (m *manager) getOrCreateSchema(schemaName string) *schema {
	schema, ok := m.schemas[schemaName]
	if !ok {
		schema = m.newSchema(schemaName)
		m.schemas[schemaName] = schema
	}
	return schema
}

func (m *manager) existsMvOrSource(schema *schema, name string) error {
	_, ok := schema.mvs[name]
	if ok {
		return fmt.Errorf("materialized view with name %s already exists in schema %s", name, schema.name)
	}
	_, ok = schema.sources[name]
	if ok {
		return fmt.Errorf("source with name %s already exists in schema %s", name, schema.name)
	}
	return nil
}

func (m *manager) ToInfoSchema() (infoschema.InfoSchema, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	return m.toInfoSchema()
}

func (m *manager) toInfoSchema() (infoschema.InfoSchema, error) {
	var schemaInfos []*SchemaInfo
	for _, schema := range m.schemas {
		tableInfos := make(map[string]*TableInfo)
		for mvName, mv := range schema.mvs {
			tableInfos[mvName] = mv.Table.Info()
		}
		for sourceName, source := range schema.sources {
			tableInfos[sourceName] = source.Table.Info()
		}
		schemaInfo := &SchemaInfo{
			SchemaName:  schema.name,
			TablesInfos: tableInfos,
		}
		schemaInfos = append(schemaInfos, schemaInfo)
	}
	return NewPranaInfoSchema(schemaInfos)
}

func (m *manager) getMaterializedView(schemaName string, name string) (mv *MaterializedView, ok bool) {
	schema, ok := m.schemas[schemaName]
	if !ok {
		return nil, false
	}
	mv, ok = schema.mvs[name]
	return mv, ok
}

func (m *manager) getSource(schemaName string, name string) (source *Source, ok bool) {
	schema, ok := m.schemas[schemaName]
	if !ok {
		return nil, false
	}
	source, ok = schema.sources[name]
	return source, ok
}

func (m *manager) buildPushQueryExecution(schema *schema, is infoschema.InfoSchema, query string) (queryDAG PushExecutorNode, err error) {
	stmt, err := m.parser.Parse(query)
	if err != nil {
		return nil, err
	}
	ctx := context.TODO()
	sessCtx := NewSessionContext()
	logicalPlan, err := m.planner.CreateLogicalPlan(ctx, sessCtx, stmt, is)
	if err != nil {
		return nil, err
	}
	physicalPlan, err := m.planner.CreatePhysicalPlan(ctx, sessCtx, logicalPlan, true, false)
	if err != nil {
		return nil, err
	}
	dag, err := BuildDAG(nil, physicalPlan, schema)
	if err != nil {
		return nil, err
	}
	dag.RecalcSchema()
	return dag, nil
}

type MaterializedView struct {
	SchemaName    string
	Name          string
	Query         string
	Table         Table
	TableExecutor *TableExecutor
	store         storage.Storage
}

func (m *MaterializedView) AddConsumingNode(node PushExecutorNode) {
	m.TableExecutor.AddConsumingNode(node)
}

type Source struct {
	SchemaName    string
	Name          string
	Table         Table
	TopicInfo     *TopicInfo
	TableExecutor *TableExecutor
}

func (s *Source) AddConsumingNode(node PushExecutorNode) {
	s.TableExecutor.AddConsumingNode(node)
}

type Sink struct {
}
