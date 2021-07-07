package common

import (
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

type Row struct {
	tRow *chunk.Row
}

type Rows struct {
	chunk *chunk.Chunk
}

type Key []interface{}

// RowsFactory caches the field types so we don't have to calculate them each time
// we create a new Rows
type RowsFactory struct {
	astFieldTypes []*types.FieldType
	ColumnTypes   []ColumnType
}

func NewRowsFactory(columnTypes []ColumnType) (*RowsFactory, error) {
	astFieldTypes, err := toAstFieldTypes(columnTypes)
	if err != nil {
		return nil, err
	}
	return &RowsFactory{astFieldTypes: astFieldTypes, ColumnTypes: columnTypes}, nil
}

func (rf *RowsFactory) NewRows(capacity int) *Rows {
	ch := chunk.NewChunkWithCapacity(rf.astFieldTypes, capacity)
	return &Rows{chunk: ch}
}

func toAstFieldTypes(columnTypes []ColumnType) ([]*types.FieldType, error) {
	var astFieldTypes []*types.FieldType
	for _, colType := range columnTypes {
		astColType, err := ConvertPranaTypeToTiDBType(colType)
		if err != nil {
			return nil, err
		}
		astFieldTypes = append(astFieldTypes, astColType)
	}
	return astFieldTypes, nil
}

func (r *Rows) GetRow(rowIndex int) Row {
	row := r.chunk.GetRow(rowIndex)
	return Row{tRow: &row}
}

func (r *Rows) RowCount() int {
	return r.chunk.NumRows()
}

func (r *Rows) AppendRow(row Row) {
	r.chunk.AppendRow(*row.tRow)
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

func (r *Rows) AppendNullToColumn(colIndex int) {
	col := r.chunk.Column(colIndex)
	col.AppendNull()
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

func (r *Row) GetFloat32(colIndex int) float32 {
	return r.tRow.GetFloat32(colIndex)
}

func (r *Row) GetDecimal(colIndex int) Decimal {
	return *NewDecimal(r.tRow.GetMyDecimal(colIndex))
}

func (r *Row) GetString(colIndex int) string {
	return r.tRow.GetString(colIndex)
}

func (r *Row) ColCount() int {
	return r.tRow.Len()
}
