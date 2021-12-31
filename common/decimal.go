package common

import (
	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/tidb/types"
)

type Decimal struct {
	decimal *types.MyDecimal
}

func ZeroDecimal() *Decimal {
	return &Decimal{decimal: &types.MyDecimal{}}
}

func NewDecimal(dec *types.MyDecimal) *Decimal {
	return &Decimal{decimal: dec}
}

func NewDecFromString(s string) (*Decimal, error) {
	dec := new(types.MyDecimal)
	if err := dec.FromString([]byte(s)); err != nil {
		return nil, errors.WithStack(err)
	}
	return &Decimal{
		decimal: dec,
	}, nil
}

func NewDecFromFloat64(f float64) (*Decimal, error) {
	dec := new(types.MyDecimal)
	if err := dec.FromFloat64(f); err != nil {
		return nil, errors.WithStack(err)
	}
	return &Decimal{
		decimal: dec,
	}, nil
}

func NewDecFromInt64(i int64) *Decimal {
	dec := new(types.MyDecimal)
	dec = dec.FromInt(i)
	return &Decimal{
		decimal: dec,
	}
}

func NewDecFromUint64(i uint64) *Decimal {
	dec := new(types.MyDecimal)
	dec = dec.FromUint(i)
	return &Decimal{
		decimal: dec,
	}
}

func (d *Decimal) CompareTo(dec *Decimal) int {
	return d.decimal.Compare(dec.decimal)
}

func (d *Decimal) Encode(buffer []byte, precision int, scale int) ([]byte, error) {
	b, err := d.decimal.WriteBin(precision, scale, buffer)
	return b, errors.WithStack(err)
}

func (d *Decimal) Decode(buffer []byte, offset int, precision int, scale int) (int, error) {
	mydec := &types.MyDecimal{}
	binSize, err := mydec.FromBin(buffer[offset:], precision, scale)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	d.decimal = mydec
	return offset + binSize, nil
}

func (d *Decimal) Add(other *Decimal) (*Decimal, error) {
	result := &types.MyDecimal{}
	if err := types.DecimalAdd(d.decimal, other.decimal, result); err != nil {
		return nil, errors.WithStack(err)
	}
	return NewDecimal(result), nil
}

func (d *Decimal) Subtract(other *Decimal) (*Decimal, error) {
	result := &types.MyDecimal{}
	if err := types.DecimalSub(d.decimal, other.decimal, result); err != nil {
		return nil, errors.WithStack(err)
	}
	return NewDecimal(result), nil
}

func (d *Decimal) String() string {
	return string(d.decimal.ToString())
}
