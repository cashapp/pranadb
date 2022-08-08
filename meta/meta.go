package meta

import (
	log "github.com/sirupsen/logrus"
	"sort"
	"sync"

	"github.com/squareup/pranadb/errors"

	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/table"
)

const (
	// SystemSchemaName is the name of the schema that houses system tables, similar to mysql's information_schema.
	SystemSchemaName = "sys"
	// TableDefTableName is the name of the table that holds all table definitions.
	TableDefTableName = "tables"
	IndexDefTableName = "indexes"
	ProtobufTableName = "protos"
)

// TableDefTableInfo is a static definition of the table schema for the table schema table.
var TableDefTableInfo = &common.MetaTableInfo{TableInfo: common.NewTableInfo(
	common.SchemaTableID,
	SystemSchemaName,
	TableDefTableName,
	[]int{0},
	[]string{"id", "kind", "schema_name", "name", "table_info", "topic_info", "query", "mv_name"},
	[]common.ColumnType{
		common.BigIntColumnType,
		common.VarcharColumnType,
		common.VarcharColumnType,
		common.VarcharColumnType,
		common.VarcharColumnType,
		common.VarcharColumnType,
		common.VarcharColumnType,
		common.VarcharColumnType,
	})}

var IndexDefTableInfo = &common.MetaTableInfo{TableInfo: common.NewTableInfo(
	common.IndexTableID,
	SystemSchemaName,
	IndexDefTableName,
	[]int{0},
	[]string{"id", "schema_name", "name", "index_info", "table_name"},
	[]common.ColumnType{
		common.BigIntColumnType,
		common.VarcharColumnType,
		common.VarcharColumnType,
		common.VarcharColumnType,
		common.VarcharColumnType,
	})}

// ProtobufTableInfo is a static definition of the table schema for the table schema table.
var ProtobufTableInfo = &common.MetaTableInfo{TableInfo: common.NewTableInfo(
	common.ProtobufTableID,
	SystemSchemaName,
	ProtobufTableName,
	[]int{0},
	[]string{"path", "fd"},
	[]common.ColumnType{
		common.VarcharColumnType,
		common.VarcharColumnType,
	})}

type Controller struct {
	lock     sync.RWMutex
	schemas  map[string]*common.Schema
	started  bool
	cluster  cluster.Cluster
	tableIDs map[uint64]struct{}
	indexIDs map[uint64]struct{}
}

func NewController(store cluster.Cluster) *Controller {
	return &Controller{
		lock:     sync.RWMutex{},
		schemas:  make(map[string]*common.Schema),
		cluster:  store,
		tableIDs: make(map[uint64]struct{}),
		indexIDs: make(map[uint64]struct{}),
	}
}

func (c *Controller) Start() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.started {
		return nil
	}
	c.registerSystemSchema()
	c.started = true
	return nil
}

func (c *Controller) Stop() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if !c.started {
		return nil
	}
	c.schemas = make(map[string]*common.Schema)
	c.tableIDs = make(map[uint64]struct{})
	c.indexIDs = make(map[uint64]struct{})
	c.started = false
	return nil
}

func (c *Controller) GetMaterializedView(schemaName string, name string) (*common.MaterializedViewInfo, bool) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	schema, ok := c.schemas[schemaName]
	if !ok {
		return nil, false
	}
	tb, ok := schema.GetTable(name)
	if !ok {
		return nil, false
	}
	mv, ok := tb.(*common.MaterializedViewInfo)
	return mv, ok
}

func (c *Controller) GetSource(schemaName string, name string) (*common.SourceInfo, bool) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	schema, ok := c.schemas[schemaName]
	if !ok {
		return nil, false
	}
	tb, ok := schema.GetTable(name)
	if !ok {
		return nil, false
	}
	source, ok := tb.(*common.SourceInfo)
	return source, ok
}

func (c *Controller) GetIndex(schemaName string, tableName string, indexName string) (*common.IndexInfo, bool) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	schema, ok := c.schemas[schemaName]
	if !ok {
		return nil, false
	}
	tb, ok := schema.GetTable(tableName)
	if !ok {
		return nil, false
	}
	indexInfos := tb.GetTableInfo().IndexInfos
	if indexInfos == nil {
		return nil, false
	}
	index, ok := indexInfos[indexName]
	return index, ok
}

func (c *Controller) GetSchemaNames() []string {
	c.lock.RLock()
	defer c.lock.RUnlock()
	var schemaNames []string
	for name := range c.schemas {
		schemaNames = append(schemaNames, name)
	}
	sort.Strings(schemaNames)
	return schemaNames
}

func (c *Controller) GetSchema(schemaName string) (schema *common.Schema, ok bool) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.getSchema(schemaName)
}

func (c *Controller) getSchema(schemaName string) (schema *common.Schema, ok bool) {
	schema, ok = c.schemas[schemaName]
	return
}

func (c *Controller) GetOrCreateSchema(schemaName string) *common.Schema {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.getOrCreateSchema(schemaName)
}

func (c *Controller) getOrCreateSchema(schemaName string) *common.Schema {
	schema, ok := c.schemas[schemaName]
	if !ok {
		schema = common.NewSchema(schemaName)
		c.schemas[schemaName] = schema
	}
	return schema
}

func (c *Controller) ExistsMvOrSource(schema *common.Schema, name string) error {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.existsTable(schema, name)
}

func (c *Controller) existsTable(schema *common.Schema, name string) error {
	if tbl, ok := schema.GetTable(name); ok {
		_, isSource := tbl.(*common.SourceInfo)
		_, isMV := tbl.(*common.MaterializedViewInfo)
		if isSource {
			return errors.NewPranaErrorf(errors.SourceAlreadyExists, "Source %s.%s already exists", schema.Name, name)
		} else if isMV {
			return errors.NewPranaErrorf(errors.MaterializedViewAlreadyExists, "Materialized view %s.%s already exists", schema.Name, name)
		} else {
			return errors.Errorf("Table %s.%s already exists", schema.Name, name)
		}
	}
	return nil
}

// RegisterIndex adds an index to the metadata controller, making it active. It does not persist it
func (c *Controller) RegisterIndex(indexInfo *common.IndexInfo) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	log.Debugf("Registering index %s with id %d", indexInfo.Name, indexInfo.ID)
	if err := c.checkIndexID(indexInfo.ID); err != nil {
		return errors.WithStack(err)
	}
	schema := c.getOrCreateSchema(indexInfo.SchemaName)
	return schema.PutIndex(indexInfo)
}

func (c *Controller) PersistIndex(indexInfo *common.IndexInfo) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	wb := cluster.NewWriteBatch(cluster.SystemSchemaShardID)
	if err := table.Upsert(IndexDefTableInfo.TableInfo, EncodeIndexInfoToRow(indexInfo), wb); err != nil {
		return errors.WithStack(err)
	}
	return c.cluster.WriteBatch(wb, false, true)
}

// UnregisterIndex removes the index from memory but does not delete it from storage
func (c *Controller) UnregisterIndex(schemaName string, tableName string, indexName string) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	schema, ok := c.schemas[schemaName]
	if !ok {
		return errors.Errorf("no such schema %s", schemaName)
	}
	tbl, _ := schema.GetTable(tableName)
	index := tbl.GetTableInfo().IndexInfos[indexName]
	err := schema.DeleteIndex(tableName, indexName)
	if err != nil {
		return err
	}
	delete(c.indexIDs, index.ID)
	return nil
}

func (c *Controller) DeleteIndex(indexID uint64) error {
	return c.deleteIndexWithID(indexID)
}

// RegisterSource adds a Source to the metadata controller, making it active. It does not persist it
func (c *Controller) RegisterSource(sourceInfo *common.SourceInfo) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	log.Debugf("Registering source %s with id %d", sourceInfo.Name, sourceInfo.ID)
	if err := c.checkTableID(sourceInfo.ID); err != nil {
		return errors.WithStack(err)
	}
	schema := c.getOrCreateSchema(sourceInfo.SchemaName)
	err := c.existsTable(schema, sourceInfo.Name)
	if err != nil {
		return errors.WithStack(err)
	}
	schema.PutTable(sourceInfo.Name, sourceInfo)
	c.tableIDs[sourceInfo.ID] = struct{}{}
	return nil
}

func (c *Controller) PersistSource(sourceInfo *common.SourceInfo) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	wb := cluster.NewWriteBatch(cluster.SystemSchemaShardID)
	if err := table.Upsert(TableDefTableInfo.TableInfo, EncodeSourceInfoToRow(sourceInfo), wb); err != nil {
		return errors.WithStack(err)
	}
	return c.cluster.WriteBatch(wb, false, true)
}

func (c *Controller) PersistMaterializedView(mvInfo *common.MaterializedViewInfo, internalTables []*common.InternalTableInfo) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	wb := cluster.NewWriteBatch(cluster.SystemSchemaShardID)
	if err := table.Upsert(TableDefTableInfo.TableInfo, EncodeMaterializedViewInfoToRow(mvInfo), wb); err != nil {
		return errors.WithStack(err)
	}
	for _, info := range internalTables {
		if err := table.Upsert(TableDefTableInfo.TableInfo, EncodeInternalTableInfoToRow(info), wb); err != nil {
			return errors.WithStack(err)
		}
	}
	return c.cluster.WriteBatch(wb, false, true)
}

func (c *Controller) checkTableID(tableID uint64) error {
	if _, ok := c.tableIDs[tableID]; ok {
		return errors.Errorf("cannot register. table with id %d already exists", tableID)
	}
	return nil
}

func (c *Controller) checkIndexID(indexID uint64) error {
	if _, ok := c.indexIDs[indexID]; ok {
		return errors.Errorf("cannot register. index with id %d already exists", indexID)
	}
	return nil
}

func (c *Controller) RegisterMaterializedView(mvInfo *common.MaterializedViewInfo, internalTables []*common.InternalTableInfo) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	log.Debugf("Registering MV %s with id %d", mvInfo.Name, mvInfo.ID)
	if err := c.checkTableID(mvInfo.ID); err != nil {
		return errors.WithStack(err)
	}
	schema := c.getOrCreateSchema(mvInfo.SchemaName)
	err := c.existsTable(schema, mvInfo.Name)
	if err != nil {
		return errors.WithStack(err)
	}
	schema.PutTable(mvInfo.Name, mvInfo)
	c.tableIDs[mvInfo.ID] = struct{}{}
	for _, internalTable := range internalTables {
		if err := c.registerInternalTable(internalTable); err != nil {
			return errors.WithStack(err)
		}
		c.tableIDs[internalTable.ID] = struct{}{}
	}
	return nil
}

func (c *Controller) registerInternalTable(info *common.InternalTableInfo) error {
	log.Debugf("Registering internal table %s with id %d", info.Name, info.ID)
	if err := c.checkTableID(info.ID); err != nil {
		return errors.WithStack(err)
	}
	schema := c.getOrCreateSchema(info.SchemaName)
	err := c.existsTable(schema, info.Name)
	if err != nil {
		return errors.WithStack(err)
	}
	schema.PutTable(info.Name, info)
	return nil
}

// UnregisterSource removes the source from memory but does not delete it from storage
func (c *Controller) UnregisterSource(schemaName string, sourceName string) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	schema, ok := c.schemas[schemaName]
	if !ok {
		return errors.Errorf("no such schema %s", schemaName)
	}
	tbl, ok := schema.GetTable(sourceName)
	if !ok {
		return errors.Errorf("no such source %s", sourceName)
	}
	if _, ok := tbl.(*common.SourceInfo); !ok {
		return errors.Errorf("%s is not a source", tbl)
	}
	delete(c.tableIDs, tbl.GetTableInfo().ID)
	schema.DeleteTable(sourceName)
	c.deleteSchemaIfEmpty(schema)
	return nil
}

func (c *Controller) DeleteSource(sourceID uint64) error {
	return c.deleteTableWithID(sourceID)
}

func (c *Controller) DeleteMaterializedView(mvInfo *common.MaterializedViewInfo, internalTableIDs []*common.InternalTableInfo) error {
	if err := c.deleteTableWithID(mvInfo.ID); err != nil {
		return errors.WithStack(err)
	}
	for _, it := range internalTableIDs {
		if err := c.deleteTableWithID(it.ID); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func (c *Controller) UnregisterMaterializedView(schemaName string, mvName string, internalTables []string) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	schema, ok := c.schemas[schemaName]
	if !ok {
		return errors.Errorf("no such schema %s", schemaName)
	}
	tbl, ok := schema.GetTable(mvName)
	if !ok {
		return errors.Errorf("no such mv %s", mvName)
	}
	if _, ok := tbl.(*common.MaterializedViewInfo); !ok {
		return errors.Errorf("%s is not a materialized view", tbl)
	}
	delete(c.tableIDs, tbl.GetTableInfo().ID)
	schema.DeleteTable(mvName)
	for _, it := range internalTables {
		internalTbl, ok := schema.GetTable(it)
		if !ok {
			return errors.Errorf("no such internal table %s", it)
		}
		if _, ok := internalTbl.(*common.InternalTableInfo); !ok {
			return errors.Errorf("%s is not an internal table", internalTbl)
		}
		delete(c.tableIDs, internalTbl.GetTableInfo().ID)
		schema.DeleteTable(it)
	}
	c.deleteSchemaIfEmpty(schema)
	return nil
}

func (c *Controller) DeleteEntityWithID(tableID uint64) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.deleteTableWithID(tableID)
}

func (c *Controller) deleteTableWithID(tableID uint64) error {
	wb := cluster.NewWriteBatch(cluster.SystemSchemaShardID)
	var key []byte
	key = table.EncodeTableKeyPrefix(common.SchemaTableID, cluster.SystemSchemaShardID, 24)
	key = common.KeyEncodeInt64(key, int64(tableID))
	wb.AddDelete(key)
	return c.cluster.WriteBatch(wb, false, true)
}

func (c *Controller) deleteIndexWithID(indexID uint64) error {
	wb := cluster.NewWriteBatch(cluster.SystemSchemaShardID)
	var key []byte
	key = table.EncodeTableKeyPrefix(common.IndexTableID, cluster.SystemSchemaShardID, 24)
	key = common.KeyEncodeInt64(key, int64(indexID))
	wb.AddDelete(key)
	return c.cluster.WriteBatch(wb, false, true)
}

func (c *Controller) registerSystemSchema() {
	schema := c.getOrCreateSchema("sys")
	schema.PutTable(TableDefTableInfo.Name, TableDefTableInfo)
	schema.PutTable(IndexDefTableInfo.Name, IndexDefTableInfo)
	schema.PutTable(ProtobufTableInfo.Name, ProtobufTableInfo)
}

// DeleteSchemaIfEmpty - Schema are removed once they have no more tables
func (c *Controller) DeleteSchemaIfEmpty(schema *common.Schema) {
	c.doDeleteSchemaIfEmpty(schema, true)
}

func (c *Controller) deleteSchemaIfEmpty(schema *common.Schema) {
	c.doDeleteSchemaIfEmpty(schema, false)
}

func (c *Controller) doDeleteSchemaIfEmpty(schema *common.Schema, lock bool) {
	if schema != nil && schema.LenTables() == 0 {
		if lock {
			c.lock.Lock()
		}
		delete(c.schemas, schema.Name)
		if lock {
			c.lock.Unlock()
		}
	}
}
