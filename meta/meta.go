package meta

import (
	"fmt"
	"sync"

	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/table"
)

const (
	// SystemSchemaName is the name of the schema that houses system tables, similar to mysql's information_schema.
	SystemSchemaName = "sys"
	// TableDefTableName is the name of the table that holds all table definitions.
	TableDefTableName      = "tables"
	SourceOffsetsTableName = "offsets"
)

// TableDefTableInfo is a static definition of the table schema for the table schema table.
var TableDefTableInfo = &common.MetaTableInfo{TableInfo: &common.TableInfo{
	ID:             common.SchemaTableID,
	SchemaName:     SystemSchemaName,
	Name:           TableDefTableName,
	PrimaryKeyCols: []int{0},
	ColumnNames:    []string{"id", "kind", "schema_name", "name", "table_info", "topic_info", "query", "mv_name"},
	ColumnTypes: []common.ColumnType{
		common.BigIntColumnType,
		common.VarcharColumnType,
		common.VarcharColumnType,
		common.VarcharColumnType,
		common.VarcharColumnType,
		common.VarcharColumnType,
		common.VarcharColumnType,
		common.VarcharColumnType,
	},
}}

var SourceOffsetsTableInfo = &common.MetaTableInfo{TableInfo: &common.TableInfo{
	ID:             common.OffsetsTableID,
	SchemaName:     SystemSchemaName,
	Name:           SourceOffsetsTableName,
	PrimaryKeyCols: []int{0, 1, 2},
	ColumnNames:    []string{"schema_name", "source_name", "partition_id", "offset"},
	// TODO need a secondary index on [schema_name, source_name] for fast lookups
	ColumnTypes: []common.ColumnType{
		common.VarcharColumnType,
		common.VarcharColumnType,
		common.BigIntColumnType,
		common.BigIntColumnType,
	},
}}

type Controller struct {
	lock    sync.RWMutex
	schemas map[string]*common.Schema
	started bool
	cluster cluster.Cluster
}

func NewController(store cluster.Cluster) *Controller {
	return &Controller{
		lock:    sync.RWMutex{},
		schemas: make(map[string]*common.Schema),
		cluster: store,
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
	c.started = false
	return nil
}

func (c *Controller) registerSystemSchema() {
	schema := c.getOrCreateSchema("sys")
	schema.PutTable(TableDefTableInfo.Name, TableDefTableInfo)
	schema.PutTable(SourceOffsetsTableInfo.Name, SourceOffsetsTableInfo)
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
	if _, ok := schema.GetTable(name); ok {
		return fmt.Errorf("table with Name %s already exists in Schema %s", name, schema.Name)
	}
	return nil
}

// RegisterSource adds a Source to the metadata controller, making it active. If persist is set, saves
// the Source schema to cluster storage.
func (c *Controller) RegisterSource(sourceInfo *common.SourceInfo, persist bool) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	schema := c.getOrCreateSchema(sourceInfo.SchemaName)
	err := c.existsTable(schema, sourceInfo.Name)
	if err != nil {
		return err
	}
	schema.PutTable(sourceInfo.Name, sourceInfo)

	if persist {
		wb := cluster.NewWriteBatch(cluster.SystemSchemaShardID, false)
		if err = table.Upsert(TableDefTableInfo.TableInfo, EncodeSourceInfoToRow(sourceInfo), wb); err != nil {
			return err
		}
		if err = c.cluster.WriteBatch(wb); err != nil {
			return err
		}
	}
	return nil
}

func (c *Controller) RegisterMaterializedView(mvInfo *common.MaterializedViewInfo, persist bool) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	schema := c.getOrCreateSchema(mvInfo.SchemaName)
	err := c.existsTable(schema, mvInfo.Name)
	if err != nil {
		return err
	}
	schema.PutTable(mvInfo.Name, mvInfo)

	if persist {
		wb := cluster.NewWriteBatch(cluster.SystemSchemaShardID, false)
		if err = table.Upsert(TableDefTableInfo.TableInfo, EncodeMaterializedViewInfoToRow(mvInfo), wb); err != nil {
			return err
		}
		if err = c.cluster.WriteBatch(wb); err != nil {
			return err
		}
	}
	return nil
}

func (c *Controller) RegisterInternalTable(info *common.InternalTableInfo, persist bool) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	schema := c.getOrCreateSchema(info.SchemaName)
	err := c.existsTable(schema, info.Name)
	if err != nil {
		return err
	}
	schema.PutTable(info.Name, info)

	if persist {
		wb := cluster.NewWriteBatch(cluster.SystemSchemaShardID, false)
		if err = table.Upsert(TableDefTableInfo.TableInfo, EncodeInternalTableInfoToRow(info), wb); err != nil {
			return err
		}
		if err = c.cluster.WriteBatch(wb); err != nil {
			return err
		}
	}
	return nil
}

func (c *Controller) RemoveSource(schemaName string, sourceName string, persist bool) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	schema, ok := c.schemas[schemaName]
	if !ok {
		return fmt.Errorf("no such schema %s", schemaName)
	}
	tbl, ok := schema.GetTable(sourceName)
	if !ok {
		return fmt.Errorf("no such source %s", sourceName)
	}
	if _, ok := tbl.(*common.SourceInfo); !ok {
		return fmt.Errorf("%s is not a source", tbl)
	}
	schema.DeleteTable(sourceName)

	if persist {
		return c.deleteEntityWIthID(tbl.GetTableInfo().ID)
	}

	return nil
}

func (c *Controller) RemoveMaterializedView(schemaName string, mvName string, persist bool) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	schema, ok := c.schemas[schemaName]
	if !ok {
		return fmt.Errorf("no such schema %s", schemaName)
	}
	tbl, ok := schema.GetTable(mvName)
	if !ok {
		return fmt.Errorf("no such mv %s", mvName)
	}
	if _, ok := tbl.(*common.MaterializedViewInfo); !ok {
		return fmt.Errorf("%s is not a materialized view", tbl)
	}
	schema.DeleteTable(mvName)

	if persist {
		return c.deleteEntityWIthID(tbl.GetTableInfo().ID)
	}

	return nil
}

func (c *Controller) deleteEntityWIthID(tableID uint64) error {
	wb := cluster.NewWriteBatch(cluster.SystemSchemaShardID, false)

	var key []byte
	key = table.EncodeTableKeyPrefix(common.SchemaTableID, cluster.SystemSchemaShardID, 24)
	key = common.KeyEncodeInt64(key, int64(tableID))

	wb.AddDelete(key)

	return c.cluster.WriteBatch(wb)
}
