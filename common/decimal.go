package common

import "github.com/pingcap/tidb/types"

type Decimal struct {
	decimal *types.MyDecimal
}

func NewDecimal(dec *types.MyDecimal) *Decimal {
	return &Decimal{decimal: dec}
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
