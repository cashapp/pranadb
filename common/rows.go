package common

import (
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

type PushRow struct {
	tRow *chunk.Row
}

type PushRows struct {
	chunk *chunk.Chunk
}

type Key []interface{}

// RowsFactory caches the field types so we don't have to calculate them each time
// we create a new PushRows
type RowsFactory struct {
	astFieldTypes []*types.FieldType
}

func NewRowsFactory(columnTypes []ColumnType) (*RowsFactory, error) {
	astFieldTypes, err := toAstFieldTypes(columnTypes)
	if err != nil {
		return nil, err
	}
	return &RowsFactory{astFieldTypes: astFieldTypes}, nil
}

func (rf *RowsFactory) NewRows(capacity int) *PushRows {
	ch := chunk.NewChunkWithCapacity(rf.astFieldTypes, capacity)
	return &PushRows{chunk: ch}
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

func (r *PushRows) GetRow(rowIndex int) PushRow {
	row := r.chunk.GetRow(rowIndex)
	return PushRow{tRow: &row}
}

func (r *PushRows) RowCount() int {
	return r.chunk.NumRows()
}

func (r *PushRows) AppendRow(row PushRow) {
	r.chunk.AppendRow(*row.tRow)
}

func (r *PushRows) AppendInt64ToColumn(colIndex int, val int64) {
	col := r.chunk.Column(colIndex)
	col.AppendInt64(val)
}

func (r *PushRows) AppendFloat64ToColumn(colIndex int, val float64) {
	col := r.chunk.Column(colIndex)
	col.AppendFloat64(val)
}

func (r *PushRows) AppendDecimalToColumn(colIndex int, val Decimal) {
	col := r.chunk.Column(colIndex)
	col.AppendMyDecimal(val.decimal)
}

func (r *PushRows) AppendStringToColumn(colIndex int, val string) {
	col := r.chunk.Column(colIndex)
	col.AppendString(val)
}

func (r *PushRows) AppendNullToColumn(colIndex int) {
	col := r.chunk.Column(colIndex)
	col.AppendNull()
}

func (r *PushRow) IsNull(colIndex int) bool {
	return r.tRow.IsNull(colIndex)
}

func (r *PushRow) GetByte(colIndex int) byte {
	return r.tRow.GetBytes(colIndex)[0]
}

func (r *PushRow) GetInt64(colIndex int) int64 {
	return r.tRow.GetInt64(colIndex)
}

func (r *PushRow) GetFloat64(colIndex int) float64 {
	return r.tRow.GetFloat64(colIndex)
}

func (r *PushRow) GetFloat32(colIndex int) float32 {
	return r.tRow.GetFloat32(colIndex)
}

func (r *PushRow) GetDecimal(colIndex int) Decimal {
	return newDecimal(r.tRow.GetMyDecimal(colIndex))
}

func (r *PushRow) GetString(colIndex int) string {
	return r.tRow.GetString(colIndex)
}

func (r *PushRow) ColCount() int {
	return r.tRow.Len()
}

type Decimal struct {
	decimal *types.MyDecimal
}

func newDecimal(dec *types.MyDecimal) Decimal {
	return Decimal{decimal: dec}
}
