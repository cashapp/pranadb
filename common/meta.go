package common

const (
	TypeTinyInt int = iota + 1
	TypeInt
	TypeBigInt
	TypeDouble
	TypeDecimal
	TypeVarchar
	TypeTimestamp
)

var TinyIntColumnType = ColumnType{TypeNumber: TypeTinyInt}
var IntColumnType = ColumnType{TypeNumber: TypeInt}
var BigIntColumnType = ColumnType{TypeNumber: TypeInt}
var DoubleColumnType = ColumnType{TypeNumber: TypeDouble}
var VarcharColumnType = ColumnType{TypeNumber: TypeVarchar}
var TimestampColumnType = ColumnType{TypeNumber: TypeTimestamp}

func NewDecimalColumnType(precision byte, scale byte) ColumnType {
	return ColumnType{
		TypeNumber:   TypeDecimal,
		DecPrecision: precision,
		DecLen:       scale,
	}
}

type ColumnType struct {
	TypeNumber   int
	DecPrecision byte
	DecLen       byte
}

type TableInfo struct {
	ID             uint64
	TableName      string
	PrimaryKeyCols []int
	ColumnNames    []string
	ColumnTypes    []ColumnType
	IndexInfos     []*IndexInfo
}

type IndexInfo struct {
	Name      string
	IndexCols []int
}

type Schema struct {
	Name    string
	Mvs     map[string]*MaterializedViewInfo
	Sources map[string]*SourceInfo
	Sinks   map[string]*SinkInfo
}

type SourceInfo struct {
	SchemaName string
	Name       string
	TableInfo  *TableInfo
	TopicInfo  *TopicInfo
}

type TopicInfo struct {
	BrokerName string
	TopicName  string
	KeyFormat  TopicEncoding
	Properties map[string]interface{}
}

type TopicEncoding int

const (
	EncodingJSON TopicEncoding = iota + 1
	EncodingProtobuf
	EncodingRaw
)

type MaterializedViewInfo struct {
	SchemaName string
	Name       string
	Query      string
	TableInfo  *TableInfo
}

type SinkInfo struct {
	Name      string
	Query     string
	TopicInfo *TopicInfo
}
