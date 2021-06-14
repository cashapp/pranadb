package sql

import (
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

type PushRow struct {
}

type PullRow struct {
	tRow *chunk.Row
}

type PullRows struct {
	chunk *chunk.Chunk
}

type Key []interface{}

// RowsFactory caches the field types so we don't have to calculate them each time
// we create a new PullRows
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

func (rf *RowsFactory) NewRows(capacity int) *PullRows {
	ch := chunk.NewChunkWithCapacity(rf.astFieldTypes, capacity)
	return &PullRows{chunk: ch}
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

func (r *PullRows) GetRow(rowIndex int) PullRow {
	row := r.chunk.GetRow(rowIndex)
	return PullRow{tRow: &row}
}

func (r *PullRows) RowCount() int {
	return r.chunk.NumRows()
}

func (r *PullRows) AppendRow(row PullRow) {
	r.chunk.AppendRow(*row.tRow)
}

func (r *PullRows) AppendInt64ToColumn(colIndex int, val int64) {
	col := r.chunk.Column(colIndex)
	col.AppendInt64(val)
}

func (r *PullRows) AppendFloat64ToColumn(colIndex int, val float64) {
	col := r.chunk.Column(colIndex)
	col.AppendFloat64(val)
}

func (r *PullRows) AppendDecimalToColumn(colIndex int, val Decimal) {
	col := r.chunk.Column(colIndex)
	col.AppendMyDecimal(val.decimal)
}

func (r *PullRows) AppendStringToColumn(colIndex int, val string) {
	col := r.chunk.Column(colIndex)
	col.AppendString(val)
}

func (r *PullRows) AppendNullToColumn(colIndex int) {
	col := r.chunk.Column(colIndex)
	col.AppendNull()
}

func (r *PullRow) IsNull(colIndex int) bool {
	return r.tRow.IsNull(colIndex)
}

func (r *PullRow) GetByte(colIndex int) byte {
	return r.tRow.GetBytes(colIndex)[0]
}

func (r *PullRow) GetInt64(colIndex int) int64 {
	return r.tRow.GetInt64(colIndex)
}

func (r *PullRow) GetFloat64(colIndex int) float64 {
	return r.tRow.GetFloat64(colIndex)
}

func (r *PullRow) GetFloat32(colIndex int) float32 {
	return r.tRow.GetFloat32(colIndex)
}

func (r *PullRow) GetDecimal(colIndex int) Decimal {
	return newDecimal(r.tRow.GetMyDecimal(colIndex))
}

func (r *PullRow) GetString(colIndex int) string {
	return r.tRow.GetString(colIndex)
}

func (r *PullRow) ColCount() int {
	return r.tRow.Len()
}

type Decimal struct {
	decimal *types.MyDecimal
}

func newDecimal(dec *types.MyDecimal) Decimal {
	return Decimal{decimal: dec}
}

type Expression struct {
	expression expression.Expression
}

func NewExpression(expression expression.Expression) *Expression {
	return &Expression{expression: expression}
}

func (e *Expression) getColumnIndex() (int, bool) {
	exp, ok := e.expression.(*expression.Column)
	if ok {
		return exp.Index, true
	} else {
		return -1, false
	}
}

func (e *Expression) EvalBoolean(row *PullRow) (bool, bool, error) {
	val, null, err := e.expression.EvalInt(nil, *row.tRow)
	return val != 0, null, err
}

func (e *Expression) EvalInt64(row *PullRow) (val int64, null bool, err error) {
	return e.expression.EvalInt(nil, *row.tRow)
}

func (e *Expression) EvalFloat64(row *PullRow) (val float64, null bool, err error) {
	return e.expression.EvalReal(nil, *row.tRow)
}

func (e *Expression) EvalDecimal(row *PullRow) (Decimal, bool, error) {
	dec, null, err := e.expression.EvalDecimal(nil, *row.tRow)
	if err != nil {
		return Decimal{}, false, err
	}
	if null {
		return Decimal{}, true, err
	}
	return newDecimal(dec), false, err
}

func (e *Expression) EvalString(row *PullRow) (val string, null bool, err error) {
	return e.expression.EvalString(nil, *row.tRow)
}
