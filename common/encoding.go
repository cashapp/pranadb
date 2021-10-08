package common

import (
	"encoding/binary"
	"math"
	"unsafe"

	"github.com/pingcap/parser/mysql"
	"github.com/squareup/pranadb/errors"
)

var littleEndian = binary.LittleEndian
var bigEndian = binary.BigEndian
var IsLittleEndian = isLittleEndian()

func AppendUint16ToBufferBE(buffer []byte, v uint16) []byte {
	return append(buffer, byte(v>>8), byte(v))
}

func AppendUint32ToBufferLE(buffer []byte, v uint32) []byte {
	return append(buffer, byte(v), byte(v>>8), byte(v>>16), byte(v>>24))
}

func AppendUint32ToBufferBE(buffer []byte, v uint32) []byte {
	return append(buffer, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

func AppendUint64ToBufferLE(buffer []byte, v uint64) []byte {
	return append(buffer, byte(v), byte(v>>8), byte(v>>16), byte(v>>24), byte(v>>32),
		byte(v>>40), byte(v>>48), byte(v>>56))
}

func AppendUint64ToBufferBE(buffer []byte, v uint64) []byte {
	return append(buffer, byte(v>>56), byte(v>>48), byte(v>>40), byte(v>>32), byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

func AppendFloat64ToBufferLE(buffer []byte, value float64) []byte {
	u := math.Float64bits(value)
	return AppendUint64ToBufferLE(buffer, u)
}

func AppendFloat64ToBufferBE(buffer []byte, value float64) []byte {
	u := math.Float64bits(value)
	return AppendUint64ToBufferBE(buffer, u)
}

func AppendFloat32ToBufferBE(buffer []byte, value float32) []byte {
	u := math.Float32bits(value)
	return AppendUint32ToBufferBE(buffer, u)
}

func AppendStringToBufferLE(buffer []byte, value string) []byte {
	buffPtr := AppendUint32ToBufferLE(buffer, uint32(len(value)))
	buffPtr = append(buffPtr, value...)
	return buffPtr
}

func AppendDecimalToBuffer(buffer []byte, dec Decimal, precision, scale int) ([]byte, error) {
	return dec.Encode(buffer, precision, scale)
}

func AppendTimestampToBuffer(buffer []byte, ts Timestamp) ([]byte, error) {
	enc, err := ts.ToPackedUint()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	buffer = AppendUint64ToBufferLE(buffer, enc)
	return buffer, nil
}

func ReadUint16FromBufferBE(buffer []byte, offset int) (uint16, int) {
	if !IsLittleEndian {
		// nolint: gosec
		return *(*uint16)(unsafe.Pointer(&buffer[offset])), offset + 2
	}
	return bigEndian.Uint16(buffer[offset:]), offset + 2
}

func ReadUint32FromBufferLE(buffer []byte, offset int) (uint32, int) {
	if IsLittleEndian {
		// nolint: gosec
		return *(*uint32)(unsafe.Pointer(&buffer[offset])), offset + 4
	}
	return littleEndian.Uint32(buffer[offset:]), offset + 4
}

func ReadUint32FromBufferBE(buffer []byte, offset int) (uint32, int) {
	if !IsLittleEndian {
		// nolint: gosec
		return *(*uint32)(unsafe.Pointer(&buffer[offset])), offset + 4
	}
	return bigEndian.Uint32(buffer[offset:]), offset + 4
}

func ReadUint64FromBufferLE(buffer []byte, offset int) (uint64, int) {
	if IsLittleEndian {
		// If architecture is little endian we can simply cast to a pointer
		// nolint: gosec
		return *(*uint64)(unsafe.Pointer(&buffer[offset])), offset + 8
	}
	return littleEndian.Uint64(buffer[offset:]), offset + 8
}

func ReadInt64FromBufferLE(buffer []byte, offset int) (int64, int) {
	u, off := ReadUint64FromBufferLE(buffer, offset)
	return int64(u), off
}

func ReadUint64FromBufferBE(buffer []byte, offset int) (uint64, int) {
	if !IsLittleEndian {
		// nolint: gosec
		return *(*uint64)(unsafe.Pointer(&buffer[offset])), offset + 8
	}
	return bigEndian.Uint64(buffer[offset:]), offset + 8
}

func ReadFloat64FromBufferLE(buffer []byte, offset int) (val float64, off int) {
	var u uint64
	u, offset = ReadUint64FromBufferLE(buffer, offset)
	val = math.Float64frombits(u)
	return val, offset
}

func ReadFloat64FromBufferBE(buffer []byte, offset int) (val float64, off int) {
	var u uint64
	u, offset = ReadUint64FromBufferBE(buffer, offset)
	val = math.Float64frombits(u)
	return val, offset
}

func ReadFloat32FromBufferBE(buffer []byte, offset int) (val float32, off int) {
	var u uint32
	u, offset = ReadUint32FromBufferBE(buffer, offset)
	val = math.Float32frombits(u)
	return val, offset
}

func ReadDecimalFromBuffer(buffer []byte, offset int, precision int, scale int) (val Decimal, off int, err error) {
	dec := Decimal{}
	offset, err = dec.Decode(buffer, offset, precision, scale)
	if err != nil {
		return Decimal{}, 0, err
	}
	return dec, offset, nil
}

func ReadTimestampFromBuffer(buffer []byte, offset int, fsp int8) (val Timestamp, off int, err error) {
	ts := Timestamp{}
	enc, off := ReadUint64FromBufferLE(buffer, offset)
	if err := ts.FromPackedUint(enc); err != nil {
		return Timestamp{}, 0, errors.WithStack(err)
	}
	ts.SetType(mysql.TypeTimestamp)
	ts.SetFsp(fsp)
	return ts, off, nil
}

func ReadStringFromBufferLE(buffer []byte, offset int) (val string, off int) {
	lu, offset := ReadUint32FromBufferLE(buffer, offset)
	l := int(lu)
	str := ByteSliceToStringZeroCopy(buffer[offset : offset+l])
	offset += l
	return str, offset
}

func ReadStringFromBufferBE(buffer []byte, offset int) (val string, off int) {
	lu, offset := ReadUint32FromBufferBE(buffer, offset)
	l := int(lu)
	str := ByteSliceToStringZeroCopy(buffer[offset : offset+l])
	offset += l
	return str, offset
}

// Are we running on a machine with a little endian architecture?
func isLittleEndian() bool {
	val := uint64(123456)
	buffer := make([]byte, 0, 8)
	buffer = AppendUint64ToBufferLE(buffer, val)
	valRead := *(*uint64)(unsafe.Pointer(&buffer[0])) // nolint: gosec
	return val == valRead
}
