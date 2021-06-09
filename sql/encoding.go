package sql

import (
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
	"unsafe"
)

// EncodeRow encodes a row to binary form for storage
func EncodeRow(row *Row, colTypes []ColumnType, buffer *[]byte) (*[]byte, error) {
	data := *buffer
	for colIndex, colType := range colTypes {
		switch colType.TypeNumber {
		case TinyIntColumnType.TypeNumber, IntColumnType.TypeNumber, BigIntColumnType.TypeNumber:
			isNull := row.IsNull(colIndex)
			if isNull {
				data = append(data, 0)
			} else {
				data = append(data, 1)
				// We store as unsigned so convert signed to unsigned
				valInt64 := row.GetInt64(colIndex)
				v := *(*uint64)(unsafe.Pointer(&valInt64))
				// Write it in little-endian
				data = *appendUint64ToBufferLittleEndian(&data, v)
			}
		case DecimalColumnType.TypeNumber:
			// TODO
		case DoubleColumnType.TypeNumber:
			isNull := row.IsNull(colIndex)
			if isNull {
				data = append(data, 0)
			} else {
				data = append(data, 1)
				val := row.GetFloat64(colIndex)
				valToStore := math.Float64bits(val)
				bytes := make([]byte, 8)
				binary.LittleEndian.PutUint64(bytes, valToStore)
				data = append(data, bytes...)
			}
		case VarcharColumnType.TypeNumber:
			isNull := row.IsNull(colIndex)
			if isNull {
				data = append(data, 0)
			} else {
				data = append(data, 1)
				val := row.GetString(colIndex)
				data = *appendUint32ToBufferLittleEndian(&data, uint32(len(val)))
				data = append(data, val...)
			}
		default:
			return nil, fmt.Errorf("unexpected column type %d", colType.TypeNumber)
		}
	}
	return &data, nil
}

func DecodeRow(dataPtr *[]byte, colTypes []ColumnType, rows *Rows) error {
	data := *dataPtr
	offset := 0
	var null bool
	var err error
	for colIndex, colType := range colTypes {
		switch colType.TypeNumber {
		case TinyIntColumnType.TypeNumber, IntColumnType.TypeNumber, BigIntColumnType.TypeNumber:
			var val int64
			val, null, offset, err = decodeInt64(data, offset)
			if err != nil {
				return err
			}
			if null {
				rows.AppendNullToColumn(colIndex)
			} else {
				rows.AppendInt64ToColumn(colIndex, val)
			}
		case DecimalColumnType.TypeNumber:
			var val Decimal
			val, null, offset, err = decodeDecimal(data, offset)
			if err != nil {
				return err
			}
			if null {
				rows.AppendNullToColumn(colIndex)
			} else {
				rows.AppendDecimalToColumn(colIndex, val)
			}
		case DoubleColumnType.TypeNumber:
			var val float64
			val, null, offset, err = decodeFloat64(data, offset)
			if err != nil {
				return err
			}
			if null {
				rows.AppendNullToColumn(colIndex)
			} else {
				rows.AppendFloat64ToColumn(colIndex, val)
			}
		case VarcharColumnType.TypeNumber:
			var val string
			val, null, offset, err = decodeString(data, offset)
			if err != nil {
				return err
			}
			if null {
				rows.AppendNullToColumn(colIndex)
			} else {
				rows.AppendStringToColumn(colIndex, val)
			}
		default:
			return fmt.Errorf("unexpected column type %d", colType.TypeNumber)
		}
	}
	return nil
}

func decodeInt64(data []byte, offset int) (val int64, null bool, off int, err error) {
	isNull := data[offset] == 0
	offset++
	if isNull {
		return 0, true, offset, nil
	}
	ptr := (*int64)(unsafe.Pointer(&data[offset]))
	val = *ptr
	offset += 8
	return val, false, offset, nil
}

func decodeFloat64(data []byte, offset int) (val float64, null bool, off int, err error) {
	isNull := data[offset] == 0
	offset++
	if isNull {
		return 0, true, offset, nil
	}
	ptr := (*float64)(unsafe.Pointer(&data[offset]))
	val = *ptr
	offset += 8
	return val, false, offset, nil
}

func decodeDecimal(data []byte, offset int) (val Decimal, null bool, off int, err error) {
	// TODO
	return Decimal{}, false, 0, nil
}

func decodeString(data []byte, offset int) (val string, null bool, off int, err error) {
	isNull := data[offset] == 0
	offset++
	if isNull {
		return "", true, offset, nil
	}
	lenPtr := (*uint32)(unsafe.Pointer(&data[offset]))
	len := int(*lenPtr)
	offset += 4
	str := toStringZeroCopy(data[offset: offset + len])
	offset += len
	return str, false, offset, nil
}

// Convert slice to string without copying
func toStringZeroCopy(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	var s string
	bytesPtr := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	stringPtr := (*reflect.StringHeader)(unsafe.Pointer(&s))
	stringPtr.Data = bytesPtr.Data
	stringPtr.Len = bytesPtr.Len
	return s
}

func appendUint32ToBufferLittleEndian(data *[]byte, v uint32) *[]byte {
	buff := append(*data, byte(v), byte(v >> 8), byte(v >> 16), byte(v >> 24))
	return &buff
}

func appendUint64ToBufferLittleEndian(data *[]byte, v uint64) *[]byte {
	buff := append(*data, byte(v), byte(v >> 8), byte(v >> 16), byte(v >> 24), byte(v >> 32),
		byte(v >> 40), byte(v >> 48), byte(v >> 56))
	return &buff
}



