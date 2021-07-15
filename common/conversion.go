package common

import (
	"fmt"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/types"
)

func ConvertPranaTypeToTiDBType(columnType ColumnType) *types.FieldType {
	switch columnType.Type {
	case TypeTinyInt:
		return types.NewFieldType(mysql.TypeTiny)
	case TypeInt:
		return types.NewFieldType(mysql.TypeLong)
	case TypeBigInt:
		return types.NewFieldType(mysql.TypeLonglong)
	case TypeDouble:
		return types.NewFieldType(mysql.TypeDouble)
	case TypeDecimal:
		return types.NewFieldType(mysql.TypeNewDecimal)
	case TypeVarchar:
		return types.NewFieldType(mysql.TypeVarchar)
	case TypeTimestamp:
		return types.NewFieldType(mysql.TypeTimestamp)
	default:
		panic(fmt.Sprintf("unknown column type %d", columnType))
	}
}

func ConvertTiDBTypeToPranaType(columnType *types.FieldType) ColumnType {
	switch columnType.Tp {
	case mysql.TypeTiny:
		return TinyIntColumnType
	case mysql.TypeLong:
		return IntColumnType
	case mysql.TypeLonglong:
		return BigIntColumnType
	case mysql.TypeDouble:
		return DoubleColumnType
	case mysql.TypeNewDecimal:
		precision := columnType.Flen
		scale := columnType.Decimal
		return NewDecimalColumnType(precision, scale)
	case mysql.TypeVarchar:
		return VarcharColumnType
	case mysql.TypeTimestamp:
		return TimestampColumnType
	default:
		panic(fmt.Sprintf("unknown colum type %d", columnType.Tp))
	}
}
