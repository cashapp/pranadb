package aggfuncs

import (
	"unsafe"

	"github.com/squareup/pranadb/common"
)

type AggState struct {
	state        []uint64
	strState     []string
	null         []bool
	decimalState []common.Decimal
	set          []bool
	changed      bool
	size         int
	extraState   [][]byte
}

func NewAggState(size int) *AggState {
	return &AggState{
		state: make([]uint64, size),
		set:   make([]bool, size),
		null:  make([]bool, size),
		size:  size,
	}
}

func (as *AggState) SetNull(index int) {
	as.set[index] = true
	as.changed = true
	as.null[index] = true
}

func (as *AggState) IsNull(index int) bool {
	return as.null[index]
}

func (as *AggState) SetInt64(index int, val int64) {
	as.set[index] = true
	as.changed = true
	ptrInt64 := (*int64)(unsafe.Pointer(&as.state[index])) // nolint: gosec
	*ptrInt64 = val
}

func (as *AggState) GetInt64(index int) int64 {
	ptrInt64 := (*int64)(unsafe.Pointer(&as.state[index])) // nolint: gosec
	return *ptrInt64
}

func (as *AggState) SetFloat64(index int, val float64) {
	as.set[index] = true
	as.changed = true
	ptrFloat64 := (*float64)(unsafe.Pointer(&as.state[index])) // nolint: gosec
	*ptrFloat64 = val
}

func (as *AggState) GetFloat64(index int) float64 {
	ptrFloat64 := (*float64)(unsafe.Pointer(&as.state[index])) // nolint: gosec
	return *ptrFloat64
}

func (as *AggState) SetString(index int, val string) {
	as.set[index] = true
	as.changed = true
	as.checkCreateStrState()
	as.strState[index] = val
}

func (as *AggState) GetString(index int) string {
	if as.strState == nil {
		return ""
	}
	return as.strState[index]
}

func (as *AggState) checkCreateStrState() {
	if as.strState == nil {
		as.strState = make([]string, as.size)
	}
}

func (as *AggState) GetExtraState(index int) []byte {
	if as.extraState == nil {
		return nil
	}
	return as.extraState[index]
}

func (as *AggState) SetExtraState(index int, extraState []byte) {
	as.set[index] = true
	as.changed = true
	as.checkCreateExtraState()
	as.extraState[index] = extraState
}

func (as *AggState) checkCreateExtraState() {
	if as.extraState == nil {
		as.extraState = make([][]byte, as.size)
	}
}

func (as *AggState) SetDecimal(index int, val common.Decimal) error {
	as.set[index] = true
	as.changed = true
	as.checkCreateDecimalState()
	as.decimalState[index] = val
	return nil
}

func (as *AggState) GetDecimal(index int) common.Decimal {
	if as.decimalState == nil {
		return *common.ZeroDecimal()
	}
	return as.decimalState[index]
}

func (as *AggState) checkCreateDecimalState() {
	if as.decimalState == nil {
		as.decimalState = make([]common.Decimal, as.size)
	}
}

func (as *AggState) SetTimestamp(index int, val common.Timestamp) error {
	as.set[index] = true
	as.changed = true
	packed, err := val.ToPackedUint()
	if err != nil {
		return err
	}
	as.SetInt64(index, int64(packed))
	return nil
}

func (as *AggState) GetTimestamp(index int) (common.Timestamp, error) {
	packed := as.GetInt64(index)
	ts := common.Timestamp{}
	if err := ts.FromPackedUint(uint64(packed)); err != nil {
		return common.Timestamp{}, err
	}
	return ts, nil
}

func (as *AggState) IsSet(index int) bool {
	return as.set[index]
}

func (as *AggState) IsChanged() bool {
	return as.changed
}
