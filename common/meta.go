package common

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

var (
	TinyIntColumnType   = ColumnType{Type: TypeTinyInt}
	IntColumnType       = ColumnType{Type: TypeInt}
	BigIntColumnType    = ColumnType{Type: TypeBigInt}
	DoubleColumnType    = ColumnType{Type: TypeDouble}
	VarcharColumnType   = ColumnType{Type: TypeVarchar}
	TimestampColumnType = ColumnType{Type: TypeTimestamp}
)

func NewDecimalColumnType(precision int, scale int) ColumnType {
	return ColumnType{
		Type:         TypeDecimal,
		DecPrecision: precision,
		DecScale:     scale,
	}
}

type ColumnType struct {
	Type         Type
	DecPrecision int
	DecScale     int
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
	EncodingUnknown TopicEncoding = iota
	EncodingJSON
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
