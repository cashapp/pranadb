//go2proto:package squareup.cash.pranadb.info
package common

//go2proto:enum
type Type uint32

// Column types.
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

var (
	TinyIntColumnType   = ColumnType{Type: TypeTinyInt}
	IntColumnType       = ColumnType{Type: TypeInt}
	BigIntColumnType    = ColumnType{Type: TypeInt}
	DoubleColumnType    = ColumnType{Type: TypeDouble}
	VarcharColumnType   = ColumnType{Type: TypeVarchar}
	TimestampColumnType = ColumnType{Type: TypeTimestamp}
)

func NewDecimalColumnType(precision byte, scale byte) ColumnType {
	return ColumnType{
		Type:         TypeDecimal,
		DecPrecision: uint32(precision),
		DecLen:       uint32(scale),
	}
}

//go2proto:message
type ColumnType struct {
	Type         Type
	DecPrecision uint32
	DecLen       uint32
}

//go2proto:message
type TableInfo struct {
	ID             uint64
	TableName      string
	PrimaryKeyCols []int
	ColumnNames    []string
	ColumnTypes    []ColumnType
	IndexInfos     []*IndexInfo
}

//go2proto:message
type IndexInfo struct {
	Name      string
	IndexCols []int
}

//go2proto:message
type Schema struct {
	Name    string
	Mvs     map[string]*MaterializedViewInfo
	Sources map[string]*SourceInfo
	Sinks   map[string]*SinkInfo
}

//go2proto:message
type SourceInfo struct {
	SchemaName string
	Name       string
	TableInfo  *TableInfo
	TopicInfo  *TopicInfo
}

//go2proto:message
type TopicInfo struct {
	BrokerName string
	TopicName  string
	KeyFormat  TopicEncoding
	Properties map[string]interface{}
}

//go2proto:enum
type TopicEncoding uint32

const (
	EncodingUnknown TopicEncoding = iota
	EncodingJSON
	EncodingProtobuf
	EncodingRaw
)

//go2proto:message
type MaterializedViewInfo struct {
	SchemaName string
	Name       string
	Query      string
	TableInfo  *TableInfo
}

//go2proto:message
type SinkInfo struct {
	Name      string
	Query     string
	TopicInfo *TopicInfo
}
