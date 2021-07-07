package common

import (
	"reflect"
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
