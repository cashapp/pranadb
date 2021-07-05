package common

import "github.com/pingcap/tidb/types"

type Decimal struct {
	decimal *types.MyDecimal
}

func NewDecimal(dec *types.MyDecimal) *Decimal {
	return &Decimal{decimal: dec}
}

func NewDecFromString(s string) (*Decimal, error) {
	dec := new(types.MyDecimal)
	if err := dec.FromString([]byte(s)); err != nil {
		return nil, err
	}
	return &Decimal{
		decimal: dec,
	}, nil
}

func NewDecFromFloat64(f float64) (*Decimal, error) {
	dec := new(types.MyDecimal)
	if err := dec.FromFloat64(f); err != nil {
		return nil, err
	}
	return &Decimal{
		decimal: dec,
	}, nil
}

func NewDecFromInt(i int64) *Decimal {
	return &Decimal{
		decimal: new(types.MyDecimal).FromInt(i),
	}
}

func NewDecFromUint(i uint64) *Decimal {
	return &Decimal{
		decimal: new(types.MyDecimal).FromUint(i),
	}
}

func (d *Decimal) Encode(buffer []byte, precision int, scale int) ([]byte, error) {
	return d.decimal.WriteBin(precision, scale, buffer)
}

func (d *Decimal) Decode(buffer []byte, offset int, precision int, scale int) (int, error) {
	mydec := &types.MyDecimal{}
	binSize, err := mydec.FromBin(buffer[offset:], precision, scale)
	if err != nil {
		return 0, err
	}
	d.decimal = mydec
	return offset + binSize, nil
}

func (d *Decimal) ToString() string {
	return string(d.decimal.ToString())
}
