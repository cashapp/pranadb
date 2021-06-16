package common

import (
	"fmt"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/types"
)

func ConvertPranaTypeToTiDBType(columnType ColumnType) (*types.FieldType, error) {
	switch columnType {
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
		return TypeTinyInt, nil
	case mysql.TypeLong:
		return TypeInt, nil
	case mysql.TypeLonglong:
		return TypeBigInt, nil
	case mysql.TypeDouble:
		return TypeDouble, nil
	case mysql.TypeNewDecimal:
		return TypeDecimal, nil
	case mysql.TypeVarchar:
		return TypeVarchar, nil
	case mysql.TypeTimestamp:
		return TypeTimestamp, nil
	default:
		return -1, fmt.Errorf("unknown colum type %d", columnType.Tp)
	}
}
