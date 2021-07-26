package common

import (
	"fmt"
	"io"
	"log"
	"reflect"
	"runtime"
	"unsafe"
)

type ByteSliceMap struct {
	TheMap map[string][]byte
}

func NewByteSliceMap() *ByteSliceMap {
	return &ByteSliceMap{TheMap: make(map[string][]byte)}
}

func (b *ByteSliceMap) Get(key []byte) (v []byte, ok bool) {
	sKey := ByteSliceToStringZeroCopy(key)
	res, ok := b.TheMap[sKey]
	return res, ok
}

func (b *ByteSliceMap) Put(key []byte, value []byte) {
	sKey := ByteSliceToStringZeroCopy(key)
	b.TheMap[sKey] = value
}

func ByteSliceToStringZeroCopy(buffer []byte) string {
	// nolint: gosec
	return *(*string)(unsafe.Pointer(&buffer))
}

func StringToByteSliceZeroCopy(str string) []byte {
	if str == "" {
		return nil
	}
	// see https://groups.google.com/g/golang-nuts/c/Zsfk-VMd_fU/m/nZoH4kExBgAJ
	const max = 0x7fff0000
	if len(str) > max {
		panic("string too long")
	}
	// nolint: gosec
	return (*[max]byte)(unsafe.Pointer((*reflect.StringHeader)(unsafe.Pointer(&str)).Data))[:len(str):len(str)]
}

// DumpStacks dumps stacks for all goroutines to stdout, useful when debugging
func DumpStacks() {
	buf := make([]byte, 1<<16)
	runtime.Stack(buf, true)
	fmt.Printf("%s", buf)
}

func InvokeCloser(closer io.Closer) {
	if closer != nil {
		err := closer.Close()
		if err != nil {
			log.Printf("failed to close closer %v", err)
		}
	}
}

// IncrementBytesLittleEndian returns a new byte slice which is 1 larger than the provided slice in little endian but without
// changing the key length
func IncrementBytesLittleEndian(bytes []byte) []byte {
	inced := CopyByteSlice(bytes)
	for i, b := range inced {
		if b < 255 {
			inced[i] = b + 1
			break
		}
		if i == len(inced)-1 {
			panic("cannot increment key - all bits set")
		}
	}
	return inced
}

func CopyByteSlice(buff []byte) []byte {
	res := make([]byte, len(buff))
	copy(res, buff)
	return res
}

func DumpDataKey(bytes []byte) string {
	if bytes == nil {
		return "nil"
	}
	if len(bytes) < 16 {
		panic("invalid key - must be at least 16 bytes")
	}
	// First 8 bytes is shard ID
	shardID := ReadUint64FromBufferBigEndian(bytes, 0)
	//Next 8 bytes is table ID
	tableID := ReadUint64FromBufferBigEndian(bytes, 8)
	//The rest depends on the table
	remaining := bytes[16:]
	return fmt.Sprintf("sid:%05d|tid:%05d|k:%v", shardID, tableID, remaining)
}
