package common

import (
	"github.com/pingcap/parser/mysql"
	"github.com/squareup/pranadb/errors"

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/types"

	"github.com/squareup/pranadb/sessctx"
)

type Expression struct {
	expression expression.Expression
	returnType *ColumnType
}

func (e *Expression) ReturnType(colTypes []ColumnType) (ColumnType, error) {
	if e.returnType == nil {
		switch op := e.expression.(type) {
		case *expression.Column:
			colIndex := op.Index
			e.returnType = &colTypes[colIndex]
		case *expression.Constant:
			colType := ConvertTiDBTypeToPranaType(op.RetType)
			e.returnType = &colType
		case *expression.ScalarFunction:
			colType := ConvertTiDBTypeToPranaType(op.RetType)
			e.returnType = &colType
		default:
			return UnknownColumnType, errors.Errorf("unexpected expr type %v", op)
		}
	}
	return *e.returnType, nil
}

func NewColumnExpression(colIndex int, colType ColumnType) *Expression {
	tiDBType := ConvertPranaTypeToTiDBType(colType)
	col := &expression.Column{RetType: tiDBType, Index: colIndex}
	return &Expression{expression: col}
}

func NewConstantInt(colType ColumnType, val int64) *Expression {
	tiDBType := ConvertPranaTypeToTiDBType(colType)
	datum := types.Datum{}
	datum.SetInt64(val)
	con := &expression.Constant{
		Value:   datum,
		RetType: tiDBType,
	}
	return &Expression{expression: con}
}

func NewConstantDouble(colType ColumnType, val float64) *Expression {
	tiDBType := ConvertPranaTypeToTiDBType(colType)
	datum := types.Datum{}
	datum.SetFloat64(val)
	con := &expression.Constant{
		Value:   datum,
		RetType: tiDBType,
	}
	return &Expression{expression: con}
}

func NewConstantVarchar(colType ColumnType, val string) *Expression {
	tiDBType := ConvertPranaTypeToTiDBType(colType)
	datum := types.Datum{}
	// This is the default collation for UTF-8, not sure it matters for our usage
	datum.SetString(val, "utf8mb4_0900_ai_ci")
	con := &expression.Constant{
		Value:   datum,
		RetType: tiDBType,
	}
	return &Expression{expression: con}
}

func NewScalarFunctionExpression(colType ColumnType, funcName string, args ...*Expression) (*Expression, error) {
	tiDBType := ConvertPranaTypeToTiDBType(colType)
	tiDBArgs := make([]expression.Expression, len(args))
	for i, ex := range args {
		tiDBArgs[i] = ex.expression
	}
	ctx := sessctx.NewDummySessionContext()
	f, err := expression.NewFunction(ctx, funcName, tiDBType, tiDBArgs...)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &Expression{expression: f}, nil
}

func NewExpression(exp expression.Expression) *Expression {
	sf, ok := exp.(*expression.ScalarFunction)
	if ok {
		// All our varchar lengths are unbounded so we override what the TiDB planner gives us
		rt := sf.RetType
		if rt.Tp == mysql.TypeVarchar {
			rt.Flen = -1
		}
	}
	return &Expression{expression: exp}
}

func (e *Expression) GetColumnIndex() (int, bool) {
	exp, ok := e.expression.(*expression.Column)
	if ok {
		return exp.Index, true
	}
	return -1, false
}

func (e *Expression) EvalBoolean(row *Row) (bool, bool, error) {
	val, null, err := e.expression.EvalInt(nil, row.tRow)
	return val != 0, null, errors.WithStack(err)
}

func (e *Expression) EvalInt64(row *Row) (val int64, null bool, err error) {
	val, null, err = e.expression.EvalInt(nil, row.tRow)
	return val, null, errors.WithStack(err)
}

func (e *Expression) EvalFloat64(row *Row) (val float64, null bool, err error) {
	val, null, err = e.expression.EvalReal(nil, row.tRow)
	return val, null, errors.WithStack(err)
}

func (e *Expression) EvalDecimal(row *Row) (Decimal, bool, error) {
	dec, null, err := e.expression.EvalDecimal(nil, row.tRow)
	if err != nil {
		return Decimal{}, false, errors.WithStack(err)
	}
	if null {
		return Decimal{}, true, errors.WithStack(err)
	}
	return *NewDecimal(dec), false, errors.WithStack(err)
}

func (e *Expression) EvalTimestamp(row *Row) (Timestamp, bool, error) {
	ts, null, err := e.expression.EvalTime(nil, row.tRow)
	if err != nil {
		return Timestamp{}, false, errors.WithStack(err)
	}
	if null {
		return Timestamp{}, true, errors.WithStack(err)
	}
	return ts, false, errors.WithStack(err)
}

func (e *Expression) EvalString(row *Row) (val string, null bool, err error) {
	return e.expression.EvalString(nil, row.tRow)
}
