package common

import (
	"fmt"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/types"
)

func ConvertPranaTypeToTiDBType(columnType ColumnType) (*types.FieldType, error) {
	switch columnType.TypeNumber {
	case TypeTinyInt:
		return types.NewFieldType(mysql.TypeTiny), nil
	case TypeInt:
		return types.NewFieldType(mysql.TypeLong), nil
	case TypeBigInt:
		return types.NewFieldType(mysql.TypeLonglong), nil
	case TypeDouble:
		return types.NewFieldType(mysql.TypeDouble), nil
	case TypeDecimal:
		return types.NewFieldType(mysql.TypeNewDecimal), nil
	case TypeVarchar:
		return types.NewFieldType(mysql.TypeVarchar), nil
	case TypeTimestamp:
		return types.NewFieldType(mysql.TypeTimestamp), nil
	default:
		return nil, fmt.Errorf("unknown colum type %d", columnType)
	}
}

func ConvertTiDBTypeToPranaType(columnType *types.FieldType) (ColumnType, error) {
	switch columnType.Tp {
	case mysql.TypeTiny:
		return TinyIntColumnType, nil
	case mysql.TypeLong:
		return IntColumnType, nil
	case mysql.TypeLonglong:
		return BigIntColumnType, nil
	case mysql.TypeDouble:
		return DoubleColumnType, nil
	case mysql.TypeNewDecimal:
		precision := columnType.Flen
		scale := columnType.Decimal
		return NewDecimalColumnType(byte(precision), byte(scale)), nil
	case mysql.TypeVarchar:
		return VarcharColumnType, nil
	case mysql.TypeTimestamp:
		return TimestampColumnType, nil
	default:
		return ColumnType{}, fmt.Errorf("unknown colum type %d", columnType.Tp)
	}
}
