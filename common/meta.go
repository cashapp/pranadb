package common

import (
	"fmt"
	"strings"
	"time"

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
		TypeTinyInt:   TinyIntColumnType,
		TypeInt:       IntColumnType,
		TypeBigInt:    BigIntColumnType,
		TypeDouble:    DoubleColumnType,
		TypeVarchar:   VarcharColumnType,
		TypeTimestamp: TimestampColumnType,
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
	case time.Time:
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

type ColumnInfo struct {
	Name string
	ColumnType
}

type ColumnType struct {
	Type         Type
	DecPrecision int
	DecScale     int
}

type TableInfo struct {
	ID             uint64
	SchemaName     string
	Name           string
	PrimaryKeyCols []int
	ColumnNames    []string
	ColumnTypes    []ColumnType
	IndexInfos     []*IndexInfo
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
	Name   string
	Tables map[string]Table
	Sinks  map[string]*SinkInfo
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
	BrokerName string
	TopicName  string
	KeyFormat  TopicEncoding
	Properties map[string]interface{}
}

type TopicEncoding int

const (
	EncodingUnknown TopicEncoding = iota
	EncodingJSON
	EncodingProtobuf
	EncodingRaw
)

type MaterializedViewInfo struct {
	*TableInfo
	Query string
}

func (i *MaterializedViewInfo) String() string {
	return "mv_" + i.TableInfo.String()
}

type SinkInfo struct {
	Name      string
	Query     string
	TopicInfo *TopicInfo
}
