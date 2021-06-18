package common

import "github.com/pingcap/tidb/expression"

type Expression struct {
	expression expression.Expression
}

func NewExpression(expression expression.Expression) *Expression {
	return &Expression{expression: expression}
}

func (e *Expression) GetColumnIndex() (int, bool) {
	exp, ok := e.expression.(*expression.Column)
	if ok {
		return exp.Index, true
	} else {
		return -1, false
	}
}

func (e *Expression) EvalBoolean(row *PushRow) (bool, bool, error) {
	val, null, err := e.expression.EvalInt(nil, *row.tRow)
	return val != 0, null, err
}

func (e *Expression) EvalInt64(row *PushRow) (val int64, null bool, err error) {
	return e.expression.EvalInt(nil, *row.tRow)
}

func (e *Expression) EvalFloat64(row *PushRow) (val float64, null bool, err error) {
	return e.expression.EvalReal(nil, *row.tRow)
}

func (e *Expression) EvalDecimal(row *PushRow) (Decimal, bool, error) {
	dec, null, err := e.expression.EvalDecimal(nil, *row.tRow)
	if err != nil {
		return Decimal{}, false, err
	}
	if null {
		return Decimal{}, true, err
	}
	return newDecimal(dec), false, err
}

func (e *Expression) EvalString(row *PushRow) (val string, null bool, err error) {
	return e.expression.EvalString(nil, *row.tRow)
}
