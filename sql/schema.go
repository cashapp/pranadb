package sql

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
	Partitions     int
}

type IndexInfo struct {
	Name      string
	IndexCols []int
}

type ColumnType int

const (
	TypeTinyInt ColumnType = iota + 1
	TypeInt
	TypeBigInt
	TypeDouble
	TypeDecimal
	TypeVarchar
	TypeTimestamp
)

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

func newSchemaInfo(name string) *SchemaInfo {
	return &SchemaInfo{
		SchemaName:  name,
		TablesInfos: map[string]*TableInfo{},
	}
}
