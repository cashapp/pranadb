package common

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

type Row struct {
	tRow        chunk.Row
	columnTypes []ColumnType
}

type Rows struct {
	tidbFieldTypes []*types.FieldType
	chunk          *chunk.Chunk
	columnTypes    []ColumnType
	codec          *chunk.Codec
}

// InferRow is a convenience function for one-off row results.
func InferRow(values ...interface{}) Row {
	columnTypes := make([]ColumnType, 0, len(values))
	for _, value := range values {
		columnTypes = append(columnTypes, InferColumnType(value))
	}
	rows := NewRows(columnTypes, 1)
	for i, value := range values {
		switch value := value.(type) {
		case string:
			rows.AppendStringToColumn(i, value)
		case int64:
			rows.AppendInt64ToColumn(i, value)
		case int:
			rows.AppendInt64ToColumn(i, int64(value))
		case int32:
			rows.AppendInt64ToColumn(i, int64(value))
		case int16:
			rows.AppendInt64ToColumn(i, int64(value))
		case float64:
			rows.AppendFloat64ToColumn(i, value)
		case Timestamp:
			rows.AppendTimestampToColumn(i, value)
		}
	}
	return rows.GetRow(0)
}

func NewRows(columnTypes []ColumnType, capacity int) *Rows {
	tidbFieldTypes := toTidbFieldTypes(columnTypes)
	ch := chunk.NewChunkWithCapacity(tidbFieldTypes, capacity)
	return &Rows{chunk: ch, columnTypes: columnTypes, tidbFieldTypes: tidbFieldTypes}
}

type Key []interface{}

// RowsFactory caches the field types so we don't have to calculate them each time
// we create a new Rows
type RowsFactory struct {
	tidbFieldTypes []*types.FieldType
	ColumnTypes    []ColumnType
}

func NewRowsFactory(columnTypes []ColumnType) *RowsFactory {
	tidbFieldTypes := toTidbFieldTypes(columnTypes)
	return &RowsFactory{tidbFieldTypes: tidbFieldTypes, ColumnTypes: columnTypes}
}

func (rf *RowsFactory) NewRows(capacity int) *Rows {
	ch := chunk.NewChunkWithCapacity(rf.tidbFieldTypes, capacity)
	return &Rows{chunk: ch, columnTypes: rf.ColumnTypes, tidbFieldTypes: rf.tidbFieldTypes}
}

func toTidbFieldTypes(columnTypes []ColumnType) []*types.FieldType {
	var astFieldTypes []*types.FieldType
	for _, colType := range columnTypes {
		astColType := ConvertPranaTypeToTiDBType(colType)
		astFieldTypes = append(astFieldTypes, astColType)
	}
	return astFieldTypes
}

func (r *Rows) ColumnTypes() []ColumnType {
	return r.columnTypes
}

func (r *Rows) GetRow(rowIndex int) Row {
	row := r.chunk.GetRow(rowIndex)
	return Row{tRow: row, columnTypes: r.columnTypes}
}

func (r *Rows) RowCount() int {
	return r.chunk.NumRows()
}

func (r *Rows) AppendRow(row Row) {
	r.chunk.AppendRow(row.tRow)
}

func (r *Rows) AppendInt64ToColumn(colIndex int, val int64) {
	col := r.chunk.Column(colIndex)
	col.AppendInt64(val)
}

func (r *Rows) AppendFloat64ToColumn(colIndex int, val float64) {
	col := r.chunk.Column(colIndex)
	col.AppendFloat64(val)
}

func (r *Rows) AppendDecimalToColumn(colIndex int, val Decimal) {
	col := r.chunk.Column(colIndex)
	col.AppendMyDecimal(val.decimal)
}

func (r *Rows) AppendStringToColumn(colIndex int, val string) {
	col := r.chunk.Column(colIndex)
	col.AppendString(val)
}

func (r *Rows) AppendTimestampToColumn(colIndex int, val Timestamp) {
	r.chunk.AppendTime(colIndex, val)
}

func (r *Rows) AppendNullToColumn(colIndex int) {
	col := r.chunk.Column(colIndex)
	col.AppendNull()
}

func (r *Rows) Serialize() []byte {
	r.checkCodec()
	if r.codec == nil {
		r.codec = chunk.NewCodec(r.tidbFieldTypes)
	}
	return r.codec.Encode(r.chunk)
}

func (r *Rows) Deserialize(buff []byte) {
	r.checkCodec()
	r.codec.DecodeToChunk(buff, r.chunk)
}

func (r *Row) IsNull(colIndex int) bool {
	return r.tRow.IsNull(colIndex)
}

func (r *Row) GetByte(colIndex int) byte {
	return r.tRow.GetBytes(colIndex)[0]
}

func (r *Row) GetInt64(colIndex int) int64 {
	return r.tRow.GetInt64(colIndex)
}

func (r *Row) GetFloat64(colIndex int) float64 {
	return r.tRow.GetFloat64(colIndex)
}

func (r *Row) GetDecimal(colIndex int) Decimal {
	return *NewDecimal(r.tRow.GetMyDecimal(colIndex))
}

func (r *Row) GetString(colIndex int) string {
	return r.tRow.GetString(colIndex)
}

func (r *Row) GetTimestamp(colIndex int) Timestamp {
	return r.tRow.GetTime(colIndex)
}

func (r *Row) ColCount() int {
	return r.tRow.Len()
}

func (r *Row) ColumnTypes() []ColumnType {
	return r.columnTypes
}

func (r *Rows) checkCodec() {
	if r.codec == nil {
		r.codec = chunk.NewCodec(r.tidbFieldTypes)
	}
}

func (r *Rows) AppendAll(other *Rows) {
	for i := 0; i < other.RowCount(); i++ {
		r.AppendRow(other.GetRow(i))
	}
}

func (r *Rows) String() string {
	var sb strings.Builder
	for i := 0; i < r.RowCount(); i++ {
		row := r.GetRow(i)
		sb.WriteString(row.String())
		if i != r.RowCount()-1 {
			sb.WriteString("\n")
		}
	}
	return sb.String()
}

func (r *Row) String() string {
	var sb strings.Builder
	sb.WriteString("|")
	for j, colType := range r.columnTypes {
		if r.IsNull(j) {
			sb.WriteString("null")
		} else {
			switch colType.Type {
			case TypeTinyInt, TypeInt, TypeBigInt:
				val := r.GetInt64(j)
				sb.WriteString(strconv.Itoa(int(val)))
			case TypeDouble:
				val := r.GetFloat64(j)
				sb.WriteString(fmt.Sprintf("%f", val))
			case TypeDecimal:
				dec := r.GetDecimal(j)
				sb.WriteString(dec.String())
			case TypeVarchar:
				dec := r.GetString(j)
				sb.WriteString(dec)
			case TypeTimestamp:
				val := r.GetTimestamp(j)
				sb.WriteString(val.String())
			default:
				panic(fmt.Sprintf("unexpected col type %d", colType.Type))
			}
		}
		sb.WriteString("|")
	}

	return sb.String()
}

// ToSimpleColNames converts a column name of the form schema.table.col to col
func ToSimpleColNames(colNames []string) []string {
	sCols := make([]string, len(colNames))
	for i, col := range colNames {
		parts := strings.Split(col, ".")
		sCols[i] = parts[len(parts)-1]
	}
	return sCols
}
