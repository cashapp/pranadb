package sql

import (
	"sync"
)

type SchemaManager interface {
	AddTable(schemaName string, tableInfo *TableInfo) (ok bool)
	RemoveTable(schemaName string, tableName string) (ok bool)
	TableExists(schemaName string, tableName string) (exists bool)
	TableByName(schemaName string, tableName string) (tableInfo *TableInfo, ok bool)
	SchemaByName(schemaName string) (schemaInfo *SchemaInfo, ok bool)
	SchemaExists(schemaName string) (exists bool)
	AllSchemas() []*SchemaInfo
}

type SchemaInfo struct {
	SchemaName  string
	TablesInfos map[string]*TableInfo
}

type TableInfo struct {
	ID          int64
	TableName   string
	ColumnInfos []*ColumnInfo
	IndexInfos  []*IndexInfo
}

type ColumnInfo struct {
	ColumnIndex int
	ColumnName  string
	ColumnType  ColumnType
}

func columnType(number byte) ColumnType {
	return ColumnType{
		TypeNumber: number,
	}
}

const (
	TypeTinyInt byte = 1
	TypeInt byte = 2
	TypeBigInt byte = 3
	TypeDouble byte = 4
	TypeDecimal byte = 5
	TypeVarchar byte = 6
	TypeTimestamp byte = 7
)

type ColumnType struct {
	TypeNumber byte
}

var TinyIntColumnType = columnType(TypeTinyInt) // 32 bit unsigned int
var IntColumnType = columnType(TypeInt) // 32 bit unsigned int
var BigIntColumnType = columnType(TypeBigInt) // 64 bit unsigned int
var DoubleColumnType = columnType(TypeDouble)
var DecimalColumnType = columnType(TypeDecimal)
var VarcharColumnType = columnType(TypeVarchar)
var TimestampColumnType = columnType(TypeTimestamp)

type IndexInfo struct {
	indexName string
}

func (t *TableInfo) Id(id int64) *TableInfo {
	t.ID = id
	return t
}

func (t *TableInfo) Name(name string) *TableInfo {
	t.TableName = name
	return t
}

func (t *TableInfo) AddColumn(name string, columnType ColumnType) *TableInfo {
	column := &ColumnInfo{
		ColumnIndex: len(t.ColumnInfos),
		ColumnName:  name,
		ColumnType:  columnType,
	}
	t.ColumnInfos = append(t.ColumnInfos, column)
	return t
}

func (t *TableInfo) AddIndex(name string) *TableInfo {
	// TODO
	return t
}

type schemaManager struct {
	lock sync.RWMutex
	schemaInfos map[string]*SchemaInfo
}

func NewSchemaManager() SchemaManager {
	return &schemaManager{
		schemaInfos: map[string]*SchemaInfo{},
	}
}

func newSchemaInfo(name string) *SchemaInfo {
	return &SchemaInfo{
		SchemaName:  name,
		TablesInfos: map[string]*TableInfo{},
	}
}

func (s* schemaManager) AddTable(schemaName string, tableInfo *TableInfo) (ok bool) {
	s.lock.Lock()
	defer s.lock.Unlock()
	schemaInfo, ok := s.schemaInfos[schemaName]
	if !ok {
		schemaInfo = newSchemaInfo(schemaName)
		s.schemaInfos[schemaName] = schemaInfo
	}
	if _, contains := schemaInfo.TablesInfos[tableInfo.TableName]; contains {
		return false
	}
	schemaInfo.TablesInfos[tableInfo.TableName] = tableInfo
	return true
}

func (s* schemaManager) RemoveTable(schemaName string, tableName string) (ok bool) {
	panic("implement me")
}

func (s* schemaManager) TableExists(schemaName string, tableName string) (exists bool) {
	panic("implement me")
}

func (s* schemaManager) TableByName(schemaName string, tableName string) (tableInfo *TableInfo, ok bool) {
	panic("implement me")
}

func (s* schemaManager) SchemaByName(schemaName string) (schemaInfo *SchemaInfo, ok bool) {
	panic("implement me")
}

func (s* schemaManager) SchemaExists(schemaName string) (exists bool) {
	panic("implement me")
}

func (s* schemaManager) AllSchemas() []*SchemaInfo {
	var schemas []*SchemaInfo
	for _, schema := range s.schemaInfos {
		schemas = append(schemas, schema)
	}
	return schemas
}

