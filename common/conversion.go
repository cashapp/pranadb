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
		ft := types.NewFieldType(mysql.TypeNewDecimal)
		ft.Flen, ft.Decimal = columnType.DecimalParameters()
		return ft
	case TypeVarchar:
		ft := types.NewFieldType(mysql.TypeVarchar)
		ft.Flen = columnType.VarcharParameters()
		return ft
	case TypeTimestamp:
		return types.NewFieldType(mysql.TypeTimestamp)
	default:
		panic(fmt.Sprintf("unknown column type %d", columnType.Type))
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
		return NewDecimalColumnType(false, precision, scale)
	case mysql.TypeVarchar:
		return NewVarcharColumnType(false, columnType.Flen)
	case mysql.TypeTimestamp:
		return TimestampColumnType
	default:
		panic(fmt.Sprintf("unknown colum type %d", columnType.Tp))
	}
}
