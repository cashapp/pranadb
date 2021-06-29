package common

import (
	"encoding/binary"
	"fmt"
	"math"
	"unsafe"
)

func EncodeCols(row *Row, colIndexes []int, colTypes []ColumnType, buffer []byte) ([]byte, error) {
	for _, colIndex := range colIndexes {
		colType := colTypes[colIndex]
		var err error
		buffer, err = EncodeCol(row, colIndex, colType, buffer)
		if err != nil {
			return nil, err
		}
	}
	return buffer, nil
}

func EncodeRow(row *Row, colTypes []ColumnType, buffer []byte) ([]byte, error) {
	for colIndex, colType := range colTypes {
		var err error
		buffer, err = EncodeCol(row, colIndex, colType, buffer)
		if err != nil {
			return nil, err
		}
	}
	return buffer, nil
}

func EncodeCol(row *Row, colIndex int, colType ColumnType, buffer []byte) ([]byte, error) {
	isNull := row.IsNull(colIndex)
	if isNull {
		buffer = append(buffer, 0)
	} else {
		buffer = append(buffer, 1)
		switch colType.TypeNumber {
		case TypeTinyInt, TypeInt, TypeBigInt:
			// We store as unsigned so convert signed to unsigned
			valInt64 := row.GetInt64(colIndex)
			buffer = EncodeInt64(valInt64, buffer)
		case TypeDecimal:
			// TODO
		case TypeDouble:
			valFloat64 := row.GetFloat64(colIndex)
			buffer = EncodeFloat64(valFloat64, buffer)
		case TypeVarchar:
			valString := row.GetString(colIndex)
			buffer = EncodeString(valString, buffer)
		default:
			return nil, fmt.Errorf("unexpected column type %d", colType)
		}
	}
	return buffer, nil
}

func EncodeKey(key Key, colTypes []ColumnType, keyColIndexes []int, buffer []byte) ([]byte, error) {
	for i, val := range key {
		var err error
		buffer, err = EncodeElement(val, colTypes[keyColIndexes[i]], buffer)
		if err != nil {
			return nil, err
		}
	}
	return buffer, nil
}

func EncodeInt64(value int64, buffer []byte) []byte {
	v := *(*uint64)(unsafe.Pointer(&value))
	// Write it in little-endian
	return AppendUint64ToBufferLittleEndian(buffer, v)
}

func EncodeFloat64(value float64, buffer []byte) []byte {
	valToStore := math.Float64bits(value)
	bytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(bytes, valToStore)
	return append(buffer, bytes...)
}

func EncodeString(value string, buffer []byte) []byte {
	buffPtr := AppendUint32ToBufferLittleEndian(buffer, uint32(len(value)))
	buffPtr = append(buffPtr, value...)
	return buffPtr
}

func EncodeElement(value interface{}, colType ColumnType, data []byte) ([]byte, error) {
	if value == nil {
		data = append(data, 0)
	} else {
		data = append(data, 1)
		switch colType.TypeNumber {
		case TypeTinyInt, TypeInt, TypeBigInt:
			valInt64 := value.(int64)
			data = EncodeInt64(valInt64, data)
		case TypeDecimal:
			// TODO
		case TypeDouble:
			valFloat64 := value.(float64)
			data = EncodeFloat64(valFloat64, data)
		case TypeVarchar:
			valString := value.(string)
			data = EncodeString(valString, data)
		default:
			return nil, fmt.Errorf("unexpected column type %d", colType)
		}
	}
	return data, nil
}

func DecodeRow(buffer []byte, colTypes []ColumnType, rows *Rows) error {
	offset := 0
	for colIndex, colType := range colTypes {
		if buffer[offset] == 0 {
			offset++
			rows.AppendNullToColumn(colIndex)
		} else {
			offset++
			switch colType.TypeNumber {
			case TypeTinyInt, TypeInt, TypeBigInt:
				var val int64
				val, offset = decodeInt64(buffer, offset)
				rows.AppendInt64ToColumn(colIndex, val)
			case TypeDecimal:
				var val Decimal
				val, offset = decodeDecimal(buffer, offset)
				rows.AppendDecimalToColumn(colIndex, val)
			case TypeDouble:
				var val float64
				val, offset = decodeFloat64(buffer, offset)
				rows.AppendFloat64ToColumn(colIndex, val)
			case TypeVarchar:
				var val string
				val, offset = decodeString(buffer, offset)
				rows.AppendStringToColumn(colIndex, val)
			default:
				return fmt.Errorf("unexpected column type %d", colType)
			}
		}
	}
	return nil
}

func decodeInt64(buffer []byte, offset int) (val int64, off int) {
	ptr := (*int64)(unsafe.Pointer(&buffer[offset]))
	val = *ptr
	offset += 8
	return val, offset
}

func decodeFloat64(buffer []byte, offset int) (val float64, off int) {
	ptr := (*float64)(unsafe.Pointer(&buffer[offset]))
	val = *ptr
	offset += 8
	return val, offset
}

func decodeDecimal(buffer []byte, offset int) (val Decimal, off int) {
	// TODO
	return Decimal{}, 0
}

func decodeString(buffer []byte, offset int) (val string, off int) {
	lenPtr := (*uint32)(unsafe.Pointer(&buffer[offset]))
	l := int(*lenPtr)
	offset += 4
	str := ByteSliceToStringZeroCopy(buffer[offset : offset+l])
	offset += l
	return str, offset
}

func AppendUint32ToBufferLittleEndian(data []byte, v uint32) []byte {
	return append(data, byte(v), byte(v>>8), byte(v>>16), byte(v>>24))
}

func AppendUint64ToBufferLittleEndian(data []byte, v uint64) []byte {
	return append(data, byte(v), byte(v>>8), byte(v>>16), byte(v>>24), byte(v>>32),
		byte(v>>40), byte(v>>48), byte(v>>56))
}
