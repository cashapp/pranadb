package meta

import (
	"fmt"
	"sync"

	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/table"
)

// SchemaTableInfo is a static definition of the table schema for the table schema table.
var SchemaTableInfo = &common.MetaTableInfo{TableInfo: &common.TableInfo{
	ID:             common.SchemaTableID,
	SchemaName:     "sys",
	Name:           "tables",
	PrimaryKeyCols: []int{0},
	ColumnNames:    []string{"id", "kind", "schema_name", "name", "table_info", "topic_info", "query"},
	ColumnTypes: []common.ColumnType{
		common.BigIntColumnType,
		common.VarcharColumnType,
		common.VarcharColumnType,
		common.VarcharColumnType,
		common.VarcharColumnType,
		common.VarcharColumnType,
		common.VarcharColumnType,
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
	if !c.started {
		return nil
	}
	if err := c.loadSchemas(); err != nil {
		return err
	}
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

func (c *Controller) loadSchemas() error {
	return nil
}

func (c *Controller) registerSystemSchema() {
	schema := c.getOrCreateSchema("sys")
	schema.Tables[SchemaTableInfo.Name] = SchemaTableInfo
}

func (c *Controller) GetMaterializedView(schemaName string, name string) (*common.MaterializedViewInfo, bool) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	schema, ok := c.schemas[schemaName]
	if !ok {
		return nil, false
	}
	tb, ok := schema.Tables[name]
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
	tb, ok := schema.Tables[name]
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
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.getOrCreateSchema(schemaName)
}

func (c *Controller) getOrCreateSchema(schemaName string) *common.Schema {
	schema, ok := c.schemas[schemaName]
	if !ok {
		schema = c.newSchema(schemaName)
		c.schemas[schemaName] = schema
	}
	return schema
}

func (c *Controller) ExistsMvOrSource(schema *common.Schema, name string) error {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.existsMvOrSource(schema, name)
}

func (c *Controller) existsMvOrSource(schema *common.Schema, name string) error {
	if _, ok := schema.Tables[name]; ok {
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
	err := c.existsMvOrSource(schema, sourceInfo.Name)
	if err != nil {
		return err
	}
	schema.Tables[sourceInfo.Name] = sourceInfo

	if persist {
		wb := cluster.NewWriteBatch(cluster.SchemaTableShardID, false)
		if err = table.Upsert(SchemaTableInfo.TableInfo, EncodeSourceInfoToRow(sourceInfo), wb); err != nil {
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
	err := c.existsMvOrSource(schema, mvInfo.Name)
	if err != nil {
		return err
	}
	schema.Tables[mvInfo.Name] = mvInfo

	if persist {
		wb := cluster.NewWriteBatch(cluster.SchemaTableShardID, false)
		if err = table.Upsert(SchemaTableInfo.TableInfo, EncodeMaterializedViewInfoToRow(mvInfo), wb); err != nil {
			return err
		}
		if err = c.cluster.WriteBatch(wb); err != nil {
			return err
		}
	}
	return nil
}

func (c *Controller) newSchema(name string) *common.Schema {
	return &common.Schema{
		Name:   name,
		Tables: make(map[string]common.Table),
		Sinks:  make(map[string]*common.SinkInfo),
	}
}

func (c *Controller) RemoveSource(schemaName string, sourceName string, persist bool) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	schema, ok := c.schemas[schemaName]
	if !ok {
		return fmt.Errorf("no such schema %s", schemaName)
	}
	tbl, ok := schema.Tables[sourceName]
	if !ok {
		return fmt.Errorf("no such source %s", sourceName)
	}
	if _, ok := tbl.(*common.SourceInfo); !ok {
		return fmt.Errorf("%s is not a source", tbl)
	}
	delete(schema.Tables, sourceName)

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
	tbl, ok := schema.Tables[mvName]
	if !ok {
		return fmt.Errorf("no such mv %s", mvName)
	}
	if _, ok := tbl.(*common.MaterializedViewInfo); !ok {
		return fmt.Errorf("%s is not a materialized view", tbl)
	}
	delete(schema.Tables, mvName)

	if persist {
		return c.deleteEntityWIthID(tbl.GetTableInfo().ID)
	}

	return nil
}

func (c *Controller) deleteEntityWIthID(tableID uint64) error {
	wb := cluster.NewWriteBatch(cluster.SchemaTableShardID, false)

	var key []byte
	key = common.AppendUint64ToBufferLittleEndian(key, common.SchemaTableID)
	key = common.AppendUint64ToBufferLittleEndian(key, cluster.SchemaTableShardID)
	key = common.AppendUint64ToBufferLittleEndian(key, tableID)

	wb.AddDelete(key)

	return c.cluster.WriteBatch(wb)
}
