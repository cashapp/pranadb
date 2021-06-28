package common

type ColumnType struct {
	TypeNumber   int
	DecPrecision byte
	DecLen       byte
}

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

type SchemaInfo struct {
	SchemaName  string
	TablesInfos map[string]*TableInfo
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

func (t *TableInfo) Id(id uint64) *TableInfo {
	t.ID = id
	return t
}

func (t *TableInfo) Name(name string) *TableInfo {
	t.TableName = name
	return t
}

func (t *TableInfo) AddColumn(name string, columnType ColumnType) *TableInfo {
	t.ColumnNames = append(t.ColumnNames, name)
	t.ColumnTypes = append(t.ColumnTypes, columnType)
	return t
}

func (t *TableInfo) AddIndex(name string) *TableInfo {
	// TODO
	return t
}
