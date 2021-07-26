package common

import (
	"encoding/binary"
	"fmt"
	"math"
	"unsafe"
)

var littleEndian = binary.LittleEndian
var bigEndian = binary.BigEndian

func EncodeCols(row *Row, colIndexes []int, colTypes []ColumnType, buffer []byte, nulls bool) ([]byte, error) {
	for _, colIndex := range colIndexes {
		colType := colTypes[colIndex]
		var err error
		buffer, err = EncodeCol(row, colIndex, colType, buffer, nulls)
		if err != nil {
			return nil, err
		}
	}
	return buffer, nil
}

func EncodeRow(row *Row, colTypes []ColumnType, buffer []byte) ([]byte, error) {
	for colIndex, colType := range colTypes {
		var err error
		buffer, err = EncodeCol(row, colIndex, colType, buffer, true)
		if err != nil {
			return nil, err
		}
	}
	return buffer, nil
}

func EncodeCol(row *Row, colIndex int, colType ColumnType, buffer []byte, nulls bool) ([]byte, error) {
	if row.IsNull(colIndex) {
		if !nulls {
			return nil, fmt.Errorf("col %d is null but nulls not supported", colIndex)
		}
		buffer = append(buffer, 0)
	} else {
		if nulls {
			buffer = append(buffer, 1)
		}
		switch colType.Type {
		case TypeTinyInt, TypeInt, TypeBigInt:
			// We store as unsigned so convert signed to unsigned
			valInt64 := row.GetInt64(colIndex)
			buffer = EncodeInt64(valInt64, buffer)
		case TypeDecimal:
			valDec := row.GetDecimal(colIndex)
			var err error
			buffer, err = valDec.Encode(buffer, colType.DecPrecision, colType.DecScale)
			if err != nil {
				return nil, err
			}
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
	for i, value := range key {
		colType := colTypes[keyColIndexes[i]]
		switch colType.Type {
		case TypeTinyInt, TypeInt, TypeBigInt:
			valInt64, ok := value.(int64)
			if !ok {
				return nil, fmt.Errorf("expected %v to be int64", value)
			}
			buffer = EncodeInt64(valInt64, buffer)
		case TypeDecimal:
			valDec, ok := value.(Decimal)
			if !ok {
				return nil, fmt.Errorf("expected %v to be Decimal", value)
			}
			return valDec.Encode(buffer, colType.DecPrecision, colType.DecScale)
		case TypeDouble:
			valFloat64, ok := value.(float64)
			if !ok {
				return nil, fmt.Errorf("expected %v to be float64", value)
			}
			buffer = EncodeFloat64(valFloat64, buffer)
		case TypeVarchar:
			valString, ok := value.(string)
			if !ok {
				return nil, fmt.Errorf("expected %v to be string", value)
			}
			buffer = EncodeString(valString, buffer)
		default:
			return nil, fmt.Errorf("unexpected column type %d", colType)
		}
	}
	return buffer, nil
}

func EncodeInt64(value int64, buffer []byte) []byte {
	// Write it in little-endian
	return AppendUint64ToBufferLittleEndian(buffer, uint64(value))
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

func DecodeRow(buffer []byte, colTypes []ColumnType, rows *Rows) error {
	offset := 0
	for colIndex, colType := range colTypes {
		if buffer[offset] == 0 {
			offset++
			rows.AppendNullToColumn(colIndex)
		} else {
			offset++
			switch colType.Type {
			case TypeTinyInt, TypeInt, TypeBigInt:
				var val int64
				val, offset = decodeInt64(buffer, offset)
				rows.AppendInt64ToColumn(colIndex, val)
			case TypeDecimal:
				var val Decimal
				var err error
				val, offset, err = DecodeDecimal(buffer, offset, colType.DecPrecision, colType.DecScale)
				if err != nil {
					return err
				}
				rows.AppendDecimalToColumn(colIndex, val)
			case TypeDouble:
				var val float64
				val, offset = DecodeFloat64(buffer, offset)
				rows.AppendFloat64ToColumn(colIndex, val)
			case TypeVarchar:
				var val string
				val, offset = DecodeString(buffer, offset)
				rows.AppendStringToColumn(colIndex, val)
			default:
				return fmt.Errorf("unexpected column type %d", colType)
			}
		}
	}
	return nil
}

func decodeInt64(buffer []byte, offset int) (val int64, off int) {
	val = int64(ReadUint64FromBufferLittleEndian(buffer, offset))
	offset += 8
	return val, offset
}

func DecodeFloat64(buffer []byte, offset int) (val float64, off int) {
	val = math.Float64frombits(ReadUint64FromBufferLittleEndian(buffer, offset))
	offset += 8
	return val, offset
}

func DecodeDecimal(buffer []byte, offset int, precision int, scale int) (val Decimal, off int, err error) {
	dec := Decimal{}
	offset, err = dec.Decode(buffer, offset, precision, scale)
	if err != nil {
		return Decimal{}, 0, err
	}
	return dec, offset, nil
}

func DecodeString(buffer []byte, offset int) (val string, off int) {
	l := int(ReadUint32FromBufferLittleEndian(buffer, offset))
	offset += 4
	str := ByteSliceToStringZeroCopy(buffer[offset : offset+l])
	offset += l
	return str, offset
}

func AppendUint32ToBufferLittleEndian(buffer []byte, v uint32) []byte {
	return append(buffer, byte(v), byte(v>>8), byte(v>>16), byte(v>>24))
}

func AppendUint64ToBufferLittleEndian(buffer []byte, v uint64) []byte {
	return append(buffer, byte(v), byte(v>>8), byte(v>>16), byte(v>>24), byte(v>>32),
		byte(v>>40), byte(v>>48), byte(v>>56))
}

func AppendUint64ToBufferBigEndian(buffer []byte, v uint64) []byte {
	return append(buffer, byte(v>>56), byte(v>>48), byte(v>>40), byte(v>>32), byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

func ReadUint32FromBufferLittleEndian(buffer []byte, offset int) uint32 {
	if IsLittleEndian {
		// nolint: gosec
		return *(*uint32)(unsafe.Pointer(&buffer[offset]))
	}
	return littleEndian.Uint32(buffer[offset:])
}

func ReadUint64FromBufferLittleEndian(buffer []byte, offset int) uint64 {
	if IsLittleEndian {
		// nolint: gosec
		return *(*uint64)(unsafe.Pointer(&buffer[offset]))
	}
	return littleEndian.Uint64(buffer[offset:])
}

func ReadUint64FromBufferBigEndian(buffer []byte, offset int) uint64 {
	if !IsLittleEndian {
		// nolint: gosec
		return *(*uint64)(unsafe.Pointer(&buffer[offset]))
	}
	return bigEndian.Uint64(buffer[offset:])
}

var IsLittleEndian = isLittleEndian()

func isLittleEndian() bool {
	val := uint64(123456)
	buffer := make([]byte, 0, 8)
	buffer = AppendUint64ToBufferLittleEndian(buffer, val)
	valRead := *(*uint64)(unsafe.Pointer(&buffer[0])) // nolint: gosec
	return val == valRead
}
