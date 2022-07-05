package common

import (
	"fmt"
	"reflect"
	"strings"
	"sync"

	"github.com/squareup/pranadb/command/parser/selector"
	"github.com/squareup/pranadb/errors"
)

type Type int

const (
	TypeUnknown Type = iota
	TypeTinyInt
	TypeInt
	TypeBigInt
	TypeDouble
	TypeDecimal
	TypeVarchar
	TypeTimestamp
)

func (t *Type) Capture(tokens []string) error {
	text := strings.ToUpper(strings.Join(tokens, " "))
	switch text {
	case "TINYINT":
		*t = TypeTinyInt
	case "INT":
		*t = TypeInt
	case "BIGINT":
		*t = TypeBigInt
	case "VARCHAR":
		*t = TypeVarchar
	case "DECIMAL":
		*t = TypeDecimal
	case "DOUBLE":
		*t = TypeDouble
	case "TIMESTAMP":
		*t = TypeTimestamp
	default:
		return errors.Errorf("unknown column type %s", text)
	}
	return nil
}

func (t Type) String() string {
	// TODO: consider using Go's stringer tool to generate this mapping.
	switch t {
	case TypeTinyInt:
		return "tinyint"
	case TypeInt:
		return "int"
	case TypeBigInt:
		return "bigint"
	case TypeDouble:
		return "double"
	case TypeDecimal:
		return "decimal"
	case TypeVarchar:
		return "varchar"
	case TypeTimestamp:
		return "timestamp"
	case TypeUnknown:
	}
	return "unknown"
}

var (
	TinyIntColumnType   = ColumnType{Type: TypeTinyInt}
	IntColumnType       = ColumnType{Type: TypeInt}
	BigIntColumnType    = ColumnType{Type: TypeBigInt}
	DoubleColumnType    = ColumnType{Type: TypeDouble}
	VarcharColumnType   = ColumnType{Type: TypeVarchar}
	TimestampColumnType = ColumnType{Type: TypeTimestamp}
	UnknownColumnType   = ColumnType{Type: TypeUnknown}

	// ColumnTypesByType allows lookup of non-parameterised ColumnType by Type.
	ColumnTypesByType = map[Type]ColumnType{
		TypeTinyInt: TinyIntColumnType,
		TypeInt:     IntColumnType,
		TypeBigInt:  BigIntColumnType,
		TypeDouble:  DoubleColumnType,
		TypeVarchar: VarcharColumnType,
	}
)

func NewDecimalColumnType(precision int, scale int) ColumnType {
	return ColumnType{
		Type:         TypeDecimal,
		DecPrecision: precision,
		DecScale:     scale,
	}
}

func NewTimestampColumnType(fsp int8) ColumnType {
	return ColumnType{
		Type: TypeTimestamp,
		FSP:  fsp,
	}
}

type ColumnInfo struct {
	Name string
	ColumnType
}

type ColumnType struct {
	Type         Type
	DecPrecision int
	DecScale     int
	FSP          int8 // fractional seconds precision for time types
}

func (t *ColumnType) String() string {
	typeName := t.Type.String()
	switch t.Type {
	case TypeDecimal:
		return fmt.Sprintf("%s(%d, %d)", typeName, t.DecPrecision, t.DecScale)
	case TypeTimestamp:
		return fmt.Sprintf("%s(%d)", typeName, t.FSP)
	default:
	}
	return typeName
}

type TableInfo struct {
	ID             uint64
	SchemaName     string
	Name           string
	PrimaryKeyCols []int
	ColumnNames    []string
	ColumnTypes    []ColumnType
	IndexInfos     map[string]*IndexInfo
	ColsVisible    []bool
	Internal       bool
	pKColsSet      map[int]struct{}
}

func (t *TableInfo) calcPKColsSet() {
	t.pKColsSet = make(map[int]struct{}, len(t.PrimaryKeyCols))
	for _, pkCol := range t.PrimaryKeyCols {
		t.pKColsSet[pkCol] = struct{}{}
	}
}

func (t *TableInfo) GetTableInfo() *TableInfo { return t }

func (t *TableInfo) String() string {
	return fmt.Sprintf("table[name=%s.%s,id=%d]", t.SchemaName, t.Name, t.ID)
}

func (t *TableInfo) IsPrimaryKeyCol(colIndex int) bool {
	if t.pKColsSet == nil {
		t.calcPKColsSet()
	}
	_, ok := t.pKColsSet[colIndex]
	return ok
}

type IndexInfo struct {
	SchemaName   string
	ID           uint64
	TableName    string
	Name         string
	IndexCols    []int
	indexColsSet map[int]struct{}
}

func (i *IndexInfo) calcColsSet() {
	i.indexColsSet = make(map[int]struct{}, len(i.IndexCols))
	for _, col := range i.IndexCols {
		i.indexColsSet[col] = struct{}{}
	}
}

func (i *IndexInfo) ContainsColIndex(colIndex int) bool {
	if i.indexColsSet == nil {
		i.calcColsSet()
	}
	_, ok := i.indexColsSet[colIndex]
	return ok
}

type Schema struct {
	// Schema can be mutated from different goroutines so we need to lock to protect access to it's maps
	lock   sync.RWMutex
	Name   string
	tables map[string]Table
	sinks  map[string]*SinkInfo
}

func NewSchema(name string) *Schema {
	return &Schema{
		Name:   name,
		tables: make(map[string]Table),
		sinks:  make(map[string]*SinkInfo),
	}
}

func (s *Schema) GetTable(name string) (Table, bool) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	t, ok := s.tables[name]
	return t, ok
}

func (s *Schema) PutIndex(indexInfo *IndexInfo) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	table, ok := s.tables[indexInfo.TableName]
	if !ok {
		return errors.Errorf("table with Name %s does not exist in Schema %s", indexInfo.TableName, s.Name)
	}
	indexInfos := table.GetTableInfo().IndexInfos
	if indexInfos == nil {
		indexInfos = map[string]*IndexInfo{}
		table.GetTableInfo().IndexInfos = indexInfos
	} else {
		_, ok := indexInfos[indexInfo.Name]
		if ok {
			return errors.Errorf("index with Name %s already exists in table %s.%s", indexInfo.Name, s.Name, indexInfo.TableName)
		}
	}
	indexInfos[indexInfo.Name] = indexInfo
	return nil
}

func (s *Schema) DeleteIndex(tableName string, indexName string) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	table, ok := s.tables[tableName]
	if !ok {
		return errors.Errorf("table with Name %s does not exist in Schema %s", tableName, s.Name)
	}
	indexInfos := table.GetTableInfo().IndexInfos
	if indexInfos != nil {
		delete(indexInfos, indexName)
	}
	return nil
}

func (s *Schema) PutTable(name string, table Table) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.tables[name] = table
}

func (s *Schema) DeleteTable(name string) {
	s.lock.Lock()
	defer s.lock.Unlock()
	delete(s.tables, name)
}

func (s *Schema) LenTables() int {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return len(s.tables)
}

func (s *Schema) GetAllTableInfos() map[string]*TableInfo {
	s.lock.RLock()
	defer s.lock.RUnlock()
	lt := len(s.tables)
	infos := make(map[string]*TableInfo, lt)
	for name, tab := range s.tables {
		infos[name] = tab.GetTableInfo()
	}
	return infos
}

func (s *Schema) Equal(other *Schema) bool {
	s.lock.RLock()
	defer s.lock.RUnlock()
	other.lock.RLock()
	defer other.lock.RUnlock()
	return reflect.DeepEqual(s, other)
}

type SourceInfo struct {
	*TableInfo
	OriginInfo *SourceOriginInfo
}

func (i *SourceInfo) String() string {
	return "source_" + i.TableInfo.String()
}

type Table interface {
	GetTableInfo() *TableInfo
}

// MetaTableInfo describes a system table that is neither a source or mv.
type MetaTableInfo struct {
	*TableInfo
}

func (i *MetaTableInfo) String() string {
	return "meta_" + i.TableInfo.String()
}

type SourceOriginInfo struct {
	BrokerName     string
	TopicName      string
	KeyEncoding    KafkaEncoding
	ValueEncoding  KafkaEncoding
	HeaderEncoding KafkaEncoding
	ColSelectors   []selector.ColumnSelector
	Properties     map[string]string
	IngestFilter   string
	InitialState   string
}

type KafkaEncoding struct {
	Encoding   Encoding
	SchemaName string
}

var (
	KafkaEncodingUnknown     = KafkaEncoding{Encoding: EncodingUnknown}
	KafkaEncodingRaw         = KafkaEncoding{Encoding: EncodingRaw}
	KafkaEncodingCSV         = KafkaEncoding{Encoding: EncodingCSV}
	KafkaEncodingJSON        = KafkaEncoding{Encoding: EncodingJSON}
	KafkaEncodingFloat32BE   = KafkaEncoding{Encoding: EncodingFloat32BE}
	KafkaEncodingFloat64BE   = KafkaEncoding{Encoding: EncodingFloat64BE}
	KafkaEncodingInt32BE     = KafkaEncoding{Encoding: EncodingInt32BE}
	KafkaEncodingInt64BE     = KafkaEncoding{Encoding: EncodingInt64BE}
	KafkaEncodingInt16BE     = KafkaEncoding{Encoding: EncodingInt16BE}
	KafkaEncodingStringBytes = KafkaEncoding{Encoding: EncodingStringBytes}
)

type Encoding int

const (
	EncodingUnknown  Encoding = iota
	EncodingRaw               // No encoding - value retained as []byte
	EncodingCSV               // Comma separated
	EncodingJSON              // JSON
	EncodingProtobuf          // Protobuf
	EncodingFloat32BE
	EncodingFloat64BE
	EncodingInt32BE
	EncodingInt64BE
	EncodingInt16BE
	EncodingStringBytes
)

// KafkaEncodingFromString decodes an encoding and an optional schema name from the string,
// in the format "<encoding>[:<schema>]". For example, for a "com.squareup.cash.Payment" protobuf,
// encoding should be specified as "protobuf:com.squareup.cash.Payment"
func KafkaEncodingFromString(str string) KafkaEncoding {
	parts := strings.SplitN(str, ":", 2)
	enc := EncodingFormatFromString(strings.ToLower(parts[0]))
	if len(parts) == 1 {
		return KafkaEncoding{Encoding: enc}
	}
	return KafkaEncoding{Encoding: enc, SchemaName: parts[1]}
}

func EncodingFormatFromString(str string) Encoding {
	str = strings.ToLower(str)
	switch str {
	case "json":
		return EncodingJSON
	case "protobuf":
		return EncodingProtobuf
	case "raw":
		return EncodingRaw
	case "csv":
		return EncodingCSV
	case "float32be":
		return EncodingFloat32BE
	case "float64be":
		return EncodingFloat64BE
	case "int32be":
		return EncodingInt32BE
	case "int64be":
		return EncodingInt64BE
	case "int16be":
		return EncodingInt16BE
	case "stringbytes":
		return EncodingStringBytes
	default:
		return EncodingUnknown
	}
}

type MaterializedViewInfo struct {
	*TableInfo
	OriginInfo *MaterializedViewOriginInfo
	Query      string
}

type MaterializedViewOriginInfo struct {
	InitialState string
}

type InternalTableInfo struct {
	*TableInfo
	// For aggregation tables that are implicit tables of materialized views with group by clauses.
	MaterializedViewName string
}

func (i *MaterializedViewInfo) String() string {
	return "mv_" + i.TableInfo.String()
}

type SinkInfo struct {
	Name      string
	Query     string
	TopicInfo *SourceOriginInfo
}
