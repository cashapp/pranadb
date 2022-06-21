package common

import (
	"math"

	"github.com/squareup/pranadb/errors"
)

/*
Keys must be encoded in a way that keys are comparable with each other as byte strings -without this
range scans in Pebble would not work properly.
We use an encoding scheme that is similar to how MySQL/RocksDB encodes keys (memcomparable)
https://github.com/facebook/mysql-5.6/wiki/MyRocks-record-format
Typically key values are stored in big-endian order
*/

const SignBitMask uint64 = 1 << 63

func KeyEncodeInt64(buffer []byte, val int64) []byte {
	uVal := uint64(val) ^ SignBitMask
	return AppendUint64ToBufferBE(buffer, uVal)
}

func KeyEncodeFloat64(buffer []byte, val float64) []byte {
	uVal := math.Float64bits(val)
	if val >= 0 {
		uVal |= SignBitMask
	} else {
		uVal = ^uVal
	}
	return AppendUint64ToBufferBE(buffer, uVal)
}

func KeyEncodeDecimal(buffer []byte, val Decimal, precision int, scale int) ([]byte, error) {
	return val.Encode(buffer, precision, scale)
}

func KeyEncodeString(buffer []byte, val string) []byte {
	buffer = AppendUint32ToBufferBE(buffer, uint32(len(val)))
	return append(buffer, val...)
}

func KeyEncodeTimestamp(buffer []byte, val Timestamp) ([]byte, error) {
	enc, err := val.ToPackedUint()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	buffer = AppendUint64ToBufferBE(buffer, enc)
	return buffer, nil
}

func EncodeKey(key Key, colTypes []ColumnType, keyColIndexes []int, buffer []byte) ([]byte, error) {
	for i, value := range key {
		colType := colTypes[keyColIndexes[i]]
		var err error
		buffer, err = EncodeKeyElement(value, colType, buffer)
		if err != nil {
			return nil, err
		}
	}
	return buffer, nil
}

func EncodeKeyElement(value interface{}, colType ColumnType, buffer []byte) ([]byte, error) {
	switch colType.Type {
	case TypeTinyInt, TypeInt, TypeBigInt:
		valInt64, ok := value.(int64)
		if !ok {
			return nil, errors.Errorf("expected %v to be int64", value)
		}
		buffer = KeyEncodeInt64(buffer, valInt64)
	case TypeDecimal:
		valDec, ok := value.(Decimal)
		if !ok {
			return nil, errors.Errorf("expected %v to be Decimal", value)
		}
		var err error
		buffer, err = KeyEncodeDecimal(buffer, valDec, colType.DecPrecision, colType.DecScale)
		if err != nil {
			return nil, errors.WithStack(err)
		}
	case TypeDouble:
		valFloat64, ok := value.(float64)
		if !ok {
			return nil, errors.Errorf("expected %v to be float64", value)
		}
		buffer = KeyEncodeFloat64(buffer, valFloat64)
	case TypeVarchar:
		valString, ok := value.(string)
		if !ok {
			return nil, errors.Errorf("expected %v to be string", value)
		}
		buffer = KeyEncodeString(buffer, valString)
	case TypeTimestamp:
		valTime, ok := value.(Timestamp)
		if !ok {
			return nil, errors.Errorf("expected %v to be Timestamp", value)
		}
		var err error
		buffer, err = KeyEncodeTimestamp(buffer, valTime)
		if err != nil {
			return nil, errors.WithStack(err)
		}
	default:
		return nil, errors.Errorf("unexpected column type %d", colType)
	}
	return buffer, nil
}

func EncodeKeyCols(row *Row, colIndexes []int, colTypes []ColumnType, buffer []byte) ([]byte, error) {
	for _, colIndex := range colIndexes {
		colType := colTypes[colIndex]
		var err error
		buffer, err = EncodeKeyCol(row, colIndex, colType, buffer)
		if err != nil {
			return nil, errors.WithStack(err)
		}
	}
	return buffer, nil
}

func EncodeIndexKeyCols(row *Row, colIndexes []int, colTypes []ColumnType, buffer []byte) ([]byte, error) {
	for _, colIndex := range colIndexes {
		colType := colTypes[colIndex]
		var err error
		if row.IsNull(colIndex) {
			buffer = append(buffer, 0)
		} else {
			buffer = append(buffer, 1)
			buffer, err = EncodeKeyCol(row, colIndex, colType, buffer)
			if err != nil {
				return nil, errors.WithStack(err)
			}
		}
	}
	return buffer, nil
}

func EncodeKeyCol(row *Row, colIndex int, colType ColumnType, buffer []byte) ([]byte, error) {
	// Key columns must be stored in big-endian so whole key can be compared byte-wise
	switch colType.Type {
	case TypeTinyInt, TypeInt, TypeBigInt:
		// We store as unsigned so convert signed to unsigned
		valInt64 := row.GetInt64(colIndex)
		buffer = KeyEncodeInt64(buffer, valInt64)
	case TypeDecimal:
		valDec := row.GetDecimal(colIndex)
		var err error
		buffer, err = KeyEncodeDecimal(buffer, valDec, colType.DecPrecision, colType.DecScale)
		if err != nil {
			return nil, errors.WithStack(err)
		}
	case TypeDouble:
		valFloat64 := row.GetFloat64(colIndex)
		buffer = KeyEncodeFloat64(buffer, valFloat64)
	case TypeVarchar:
		valString := row.GetString(colIndex)
		buffer = KeyEncodeString(buffer, valString)
	case TypeTimestamp:
		valTime := row.GetTimestamp(colIndex)
		var err error
		buffer, err = KeyEncodeTimestamp(buffer, valTime)
		if err != nil {
			return nil, errors.WithStack(err)
		}
	default:
		return nil, errors.Errorf("unexpected column type %d", colType)
	}
	return buffer, nil
}

func DecodeIndexOrPKCols(buffer []byte, offset int, pk bool, indexOrPKColTypes []ColumnType, indexOrPKOutputCols []int, rows *Rows) (int, error) {
	for i, outputCol := range indexOrPKOutputCols {
		colType := indexOrPKColTypes[i]
		var err error
		offset, err = DecodeIndexOrPKCol(buffer, offset, colType, outputCol, pk, rows)
		if err != nil {
			return 0, err
		}
	}
	return offset, nil
}

func DecodeIndexOrPKCol(buffer []byte, offset int, colType ColumnType, outputColIndex int, pkCol bool, rows *Rows) (int, error) {
	isNull := false
	if !pkCol {
		isNull = buffer[offset] == 0
		offset++
	}
	if isNull {
		if outputColIndex != -1 {
			rows.AppendNullToColumn(outputColIndex)
		}
	} else {
		switch colType.Type {
		case TypeTinyInt, TypeInt, TypeBigInt:
			var u uint64
			u, offset = ReadUint64FromBufferBE(buffer, offset)
			if outputColIndex != -1 {
				decoded := u ^ SignBitMask
				rows.AppendInt64ToColumn(outputColIndex, int64(decoded))
			}
		case TypeDecimal:
			var val Decimal
			var err error
			val, offset, err = ReadDecimalFromBuffer(buffer, offset, colType.DecPrecision, colType.DecScale)
			if err != nil {
				return 0, errors.WithStack(err)
			}
			if outputColIndex != -1 {
				rows.AppendDecimalToColumn(outputColIndex, val)
			}
		case TypeDouble:
			var val float64
			val, offset = ReadFloat64FromBufferBE(buffer, offset)
			if outputColIndex != -1 {
				rows.AppendFloat64ToColumn(outputColIndex, val)
			}
		case TypeVarchar:
			var val string
			val, offset = ReadStringFromBufferBE(buffer, offset)
			if outputColIndex != -1 {
				rows.AppendStringToColumn(outputColIndex, val)
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
			if outputColIndex != -1 {
				rows.AppendTimestampToColumn(outputColIndex, val)
			}
		default:
			return 0, errors.Errorf("unexpected column type %d", colType)
		}
	}
	return offset, nil
}
