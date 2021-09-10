package source

import (
	"fmt"
	"math"
	"reflect"
	"strconv"
	"time"

	"github.com/squareup/pranadb/common"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func CoerceInt64(val interface{}) (int64, error) {
	switch v := val.(type) {
	case int64:
		return v, nil
	case int32:
		return int64(v), nil
	case uint64:
		if v > math.MaxInt64 {
			return 0, fmt.Errorf("value %d is too large to be coerced to int64", v)
		}
		return int64(v), nil
	case uint32:
		return int64(v), nil
	case uint16:
		return int64(v), nil
	case int16:
		return int64(v), nil
	case int:
		if v > math.MaxInt64 {
			return 0, fmt.Errorf("value %d is too large to be coerced to int64", v)
		}
		return int64(v), nil
	case float32:
		return int64(v), nil
	case float64:
		return int64(v), nil
	case bool:
		if v {
			return 1, nil
		}
		return 0, nil
	case string:
		r, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("string value %s cannot be coerced to int64 %v", v, err)
		}
		return r, nil
	default:
		return 0, coerceFailedErr(v, "int64")
	}
}

func CoerceFloat64(val interface{}) (float64, error) {
	switch v := val.(type) {
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case uint64:
		return float64(v), nil
	case uint32:
		return float64(v), nil
	case uint16:
		return float64(v), nil
	case int16:
		return float64(v), nil
	case int:
		return float64(v), nil
	case bool:
		if v {
			return 1, nil
		}
		return 0, nil
	case string:
		r, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return 0, fmt.Errorf("string value %s cannot be coerced to float64 %v", v, err)
		}
		return r, nil
	default:
		return 0, coerceFailedErr(v, "float64")
	}
}

func CoerceString(val interface{}) (string, error) {
	switch v := val.(type) {
	case string:
		return v, nil
	case int64, int32, uint64, int16, uint32, uint16, int:
		return fmt.Sprintf("%d", v), nil
	case float64, float32:
		return fmt.Sprintf("%f", v), nil
	case common.Decimal:
		return v.String(), nil
	default:
		return "", coerceFailedErr(v, "string")
	}
}

func CoerceDecimal(val interface{}) (*common.Decimal, error) {
	switch v := val.(type) {
	case *common.Decimal:
		return v, nil
	case string:
		return common.NewDecFromString(v)
	case int64:
		return common.NewDecFromInt64(v), nil
	case int32:
		return common.NewDecFromInt64(int64(v)), nil
	case uint64:
		return common.NewDecFromUint64(v), nil
	case uint32:
		return common.NewDecFromUint64(uint64(v)), nil
	case uint16:
		return common.NewDecFromUint64(uint64(v)), nil
	case int16:
		return common.NewDecFromUint64(uint64(v)), nil
	case float64:
		return common.NewDecFromFloat64(v)
	case float32:
		return common.NewDecFromFloat64(float64(v))
	default:
		return nil, coerceFailedErr(v, "decimal")
	}
}

func CoerceTimestamp(val interface{}) (common.Timestamp, error) {
	switch v := val.(type) {
	case *common.Timestamp:
		return *v, nil
	case time.Time:
		return common.NewTimestampFromGoTime(v), nil
	case string:
		return common.NewTimestampFromString(v), nil
	case float64:
		return CoerceTimestamp(uint64(v))
	case uint64:
		// Incoming value is assumed to be Unix milliseconds past epoch
		ts := common.NewTimestampFromUnixEpochMillis(int64(v))
		return ts, nil
	case *timestamppb.Timestamp:
		return common.NewTimestampFromGoTime(v.AsTime()), nil
	case timestamppb.Timestamp:
		return common.NewTimestampFromGoTime(v.AsTime()), nil
	default:
		return common.Timestamp{}, coerceFailedErr(v, "timestamp")
	}
}

func coerceFailedErr(v interface{}, t string) error {
	return fmt.Errorf("cannot coerce value %v, type %s to %s", v, reflect.TypeOf(v), t)
}
