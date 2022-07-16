package common

import (
	"fmt"

	"github.com/pingcap/parser/mysql"
	"github.com/squareup/pranadb/tidb/types"
)

func ConvertPranaTypeToTiDBType(columnType ColumnType) *types.FieldType {
	var ft *types.FieldType
	switch columnType.Type {
	case TypeTinyInt:
		ft = types.NewFieldType(mysql.TypeTiny)
	case TypeInt:
		ft = types.NewFieldType(mysql.TypeLong)
	case TypeBigInt:
		ft = types.NewFieldType(mysql.TypeLonglong)
	case TypeDouble:
		ft = types.NewFieldType(mysql.TypeDouble)
	case TypeDecimal:
		ft = types.NewFieldType(mysql.TypeNewDecimal)
		ft.Flen = columnType.DecPrecision
		ft.Decimal = columnType.DecScale
	case TypeVarchar:
		ft = types.NewFieldType(mysql.TypeVarchar)
	case TypeTimestamp:
		ft = types.NewFieldType(mysql.TypeTimestamp)
		ft.Decimal = int(columnType.FSP)
	default:
		panic(fmt.Sprintf("unknown column type %d", columnType))
	}
	ft.Collate = mysql.DefaultCollationName // Need to set this to avoid TiDB warnings in log
	return ft
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
		return NewDecimalColumnType(columnType.Flen, columnType.Decimal), nil
	case mysql.TypeVarchar, mysql.TypeVarString:
		return VarcharColumnType, nil
	case mysql.TypeTimestamp:
		return ColumnType{Type: TypeTimestamp, FSP: int8(columnType.Decimal)}, nil
	default:
		return ColumnType{}, fmt.Errorf("unexpected colum type %d", columnType.Tp)
	}
}

func TiDBValueToPranaValue(tidbValue interface{}) interface{} {
	mydec, ok := tidbValue.(*types.MyDecimal)
	if ok {
		return *NewDecimal(mydec)
	}
	return tidbValue
}
