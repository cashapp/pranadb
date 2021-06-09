package sql

import (
	"fmt"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/types"
)

func ConvertColumnType(columnType ColumnType) (*types.FieldType, error) {
	switch columnType {
	case TinyIntColumnType:
		return types.NewFieldType(mysql.TypeTiny), nil
	case IntColumnType:
		return types.NewFieldType(mysql.TypeLong), nil
	case BigIntColumnType:
		return types.NewFieldType(mysql.TypeLonglong), nil
	case DoubleColumnType:
		return types.NewFieldType(mysql.TypeDouble), nil
	case DecimalColumnType:
		return types.NewFieldType(mysql.TypeNewDecimal), nil
	case VarcharColumnType:
		return types.NewFieldType(mysql.TypeVarchar), nil
	case TimestampColumnType:
		return types.NewFieldType(mysql.TypeTimestamp), nil
	default:
		return nil, fmt.Errorf("unknown colum type %d", columnType.TypeNumber)
	}
}


