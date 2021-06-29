package meta

import (
	"fmt"
	"github.com/pingcap/tidb/infoschema"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/parplan"
	"sync"
)

type Controller struct {
	lock    sync.RWMutex
	schemas map[string]*common.Schema
}

func NewController() *Controller {
	return &Controller{
		lock:    sync.RWMutex{},
		schemas: make(map[string]*common.Schema),
	}
}

type ISSchemaInfo struct {
	SchemaName  string
	TablesInfos map[string]*common.TableInfo
}

func (c *Controller) Start() error {
	// TODO load all state from storage
	return nil
}

func (c *Controller) Stop() error {
	return nil
}

func (c *Controller) GetMaterializedView(schemaName string, name string) (mv *common.MaterializedViewInfo, ok bool) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	schema, ok := c.schemas[schemaName]
	if !ok {
		return nil, false
	}
	mv, ok = schema.Mvs[name]
	return mv, ok
}

func (c *Controller) GetSource(schemaName string, name string) (source *common.SourceInfo, ok bool) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	schema, ok := c.schemas[schemaName]
	if !ok {
		return nil, false
	}
	source, ok = schema.Sources[name]
	return source, ok
}

func (c *Controller) GetInfoSchema() (infoschema.InfoSchema, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	var schemaInfos []*ISSchemaInfo
	for _, schema := range c.schemas {
		tableInfos := make(map[string]*common.TableInfo)
		for mvName, mv := range schema.Mvs {
			tableInfos[mvName] = mv.TableInfo
		}
		for sourceName, source := range schema.Sources {
			tableInfos[sourceName] = source.TableInfo
		}
		schemaInfo := &ISSchemaInfo{
			SchemaName:  schema.Name,
			TablesInfos: tableInfos,
		}
		schemaInfos = append(schemaInfos, schemaInfo)
	}
	return parplan.NewPranaInfoSchema(schemaInfos)
}

func (c *Controller) GetSchema(schemaName string) (schema *common.Schema, ok bool) {
	c.lock.RLock()
	defer c.lock.RUnlock()
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

func (c *Controller) RegisterSource(sourceInfo *common.SourceInfo) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	schema, ok := c.GetSchema(sourceInfo.SchemaName)
	if !ok {
		return fmt.Errorf("no such schema %s", schema.Name)
	}
	err := c.existsMvOrSource(schema, sourceInfo.Name)
	if err != nil {
		return err
	}
	schema.Sources[sourceInfo.Name] = sourceInfo
	return nil
}

func (c *Controller) RegisterMaterializedView(mvInfo *common.MaterializedViewInfo) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	schema, ok := c.GetSchema(mvInfo.SchemaName)
	if !ok {
		return fmt.Errorf("no such schema %s", schema.Name)
	}
	err := c.existsMvOrSource(schema, mvInfo.Name)
	if err != nil {
		return err
	}
	schema.Mvs[mvInfo.Name] = mvInfo
	return nil
}

func (c *Controller) newSchema(name string) *common.Schema {
	return &common.Schema{
		Name:    name,
		Mvs:     make(map[string]*common.MaterializedViewInfo),
		Sources: make(map[string]*common.SourceInfo),
		Sinks:   make(map[string]*common.SinkInfo),
	}
}
