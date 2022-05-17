package common

import (
	"github.com/squareup/pranadb/errors"
)

// Values in rows are typically encoded in little-endian order
// Most CPU architectures are little-endian so those allows us to simply cast values in the cast of int types

func EncodeRow(row *Row, colTypes []ColumnType, buffer []byte) ([]byte, error) {
	for colIndex, colType := range colTypes {
		var err error
		buffer, err = encodeRowCol(row, colIndex, colType, buffer)
		if err != nil {
			return nil, errors.WithStack(err)
		}
	}
	return buffer, nil
}

func encodeRowCol(row *Row, colIndex int, colType ColumnType, buffer []byte) ([]byte, error) {
	if row.IsNull(colIndex) {
		buffer = append(buffer, 0)
	} else {
		buffer = append(buffer, 1)
		switch colType.Type {
		case TypeTinyInt, TypeInt, TypeBigInt:
			// We store as unsigned so convert signed to unsigned
			valInt64 := row.GetInt64(colIndex)
			buffer = AppendUint64ToBufferLE(buffer, uint64(valInt64))
		case TypeDecimal:
			valDec := row.GetDecimal(colIndex)
			var err error
			buffer, err = valDec.Encode(buffer, colType.DecPrecision, colType.DecScale)
			if err != nil {
				return nil, errors.WithStack(err)
			}
		case TypeDouble:
			valFloat64 := row.GetFloat64(colIndex)
			buffer = AppendFloat64ToBufferLE(buffer, valFloat64)
		case TypeVarchar:
			valString := row.GetString(colIndex)
			buffer = AppendStringToBufferLE(buffer, valString)
		case TypeTimestamp:
			valTimestamp := row.GetTimestamp(colIndex)
			var err error
			buffer, err = AppendTimestampToBuffer(buffer, valTimestamp)
			if err != nil {
				return nil, errors.WithStack(err)
			}
		default:
			return nil, errors.Errorf("unexpected column type %d", colType)
		}
	}
	return buffer, nil
}

func DecodeRow(buffer []byte, colTypes []ColumnType, rows *Rows) error {
	return DecodeRowWithIgnoredCols(buffer, colTypes, nil, rows)
}

func DecodeRowWithIgnoredCols(buffer []byte, colTypes []ColumnType, includeCol []bool, rows *Rows) error {
	offset := 0
	colIndex := 0
	var err error
	for i, colType := range colTypes {
		include := includeCol == nil || includeCol[i]
		offset, err = DecodeRowCol(buffer, offset, rows, colType, colIndex, include)
		if err != nil {
			return errors.WithStack(err)
		}
		if include {
			colIndex++
		}
	}
	return nil
}

func DecodeRowCol(buffer []byte, offset int, rows *Rows, colType ColumnType, colIndex int, include bool) (int, error) {
	if buffer[offset] == 0 {
		offset++
		if include {
			rows.AppendNullToColumn(colIndex)
		}
	} else {
		offset++
		switch colType.Type {
		case TypeTinyInt, TypeInt, TypeBigInt:
			var u uint64
			u, offset = ReadUint64FromBufferLE(buffer, offset)
			if include {
				rows.AppendInt64ToColumn(colIndex, int64(u))
			}
		case TypeDecimal:
			var val Decimal
			var err error
			val, offset, err = ReadDecimalFromBuffer(buffer, offset, colType.DecPrecision, colType.DecScale)
			if err != nil {
				return 0, errors.WithStack(err)
			}
			if include {
				rows.AppendDecimalToColumn(colIndex, val)
			}
		case TypeDouble:
			var val float64
			val, offset = ReadFloat64FromBufferLE(buffer, offset)
			if include {
				rows.AppendFloat64ToColumn(colIndex, val)
			}
		case TypeVarchar:
			var val string
			val, offset = ReadStringFromBufferLE(buffer, offset)
			if include {
				rows.AppendStringToColumn(colIndex, val)
			}
		case TypeTimestamp:
			var (
				val Timestamp
				err error
			)
			val, offset, err = ReadTimestampFromBuffer(buffer, offset, colType.FSP)
			if err != nil {
				return 0, errors.WithStack(err)
			}
			if include {
				rows.AppendTimestampToColumn(colIndex, val)
			}
		default:
			return 0, errors.Errorf("unexpected column type %d", colType)
		}
	}
	return offset, nil
}
