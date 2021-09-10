package common

import (
	"fmt"
	"reflect"
	"strings"
	"sync"

	"github.com/pkg/errors"
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

// InferColumnType from Go type.
func InferColumnType(value interface{}) ColumnType {
	switch value.(type) {
	case string:
		return VarcharColumnType
	case int, int64:
		return BigIntColumnType
	case int16, int32:
		return IntColumnType
	case int8:
		return TinyIntColumnType
	case float64:
		return DoubleColumnType
	case Timestamp:
		return TimestampColumnType
	default:
		panic(fmt.Sprintf("can't infer column of type %T", value))
	}
}

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

type TableInfo struct {
	ID             uint64
	SchemaName     string
	Name           string
	PrimaryKeyCols []int
	ColumnNames    []string
	ColumnTypes    []ColumnType
	IndexInfos     []*IndexInfo
	ColsVisible    []bool
}

func (i *TableInfo) GetTableInfo() *TableInfo { return i }

func (i *TableInfo) String() string {
	return fmt.Sprintf("table[name=%s.%s,id=%d]", i.SchemaName, i.Name, i.ID)
}

type IndexInfo struct {
	Name      string
	IndexCols []int
}

type Schema struct {
	// Schema can be mutated from different goroutines so we need to lock to protect access to it's maps
	lock    sync.RWMutex
	Name    string
	tables  map[string]Table
	sinks   map[string]*SinkInfo
	deleted bool
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

func (s *Schema) SetDeleted() {
	s.lock.RLock()
	defer s.lock.RUnlock()
	s.deleted = true
}

func (s *Schema) IsDeleted() bool {
	// Schema cna become deleted if all tables are removed, but sessions might still have it cached
	// checking isDeleted() allows sessions to refresh the schema if necessary
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.deleted
}

type SourceInfo struct {
	*TableInfo
	TopicInfo *TopicInfo
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

type TopicInfo struct {
	BrokerName     string
	TopicName      string
	KeyEncoding    KafkaEncoding
	ValueEncoding  KafkaEncoding
	HeaderEncoding KafkaEncoding
	ColSelectors   []string
	Properties     map[string]string
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

func KafkaEncodingProtobuf(schema string) KafkaEncoding {
	return KafkaEncoding{Encoding: EncodingProtobuf, SchemaName: schema}
}

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
	Query string
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
	TopicInfo *TopicInfo
}
