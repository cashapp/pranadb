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
	return *(*string)(unsafe.Pointer(&buffer))
}

func StringToByteSliceZeroCopy(str string) []byte {
	var buffer = *(*[]byte)(unsafe.Pointer(&str))
	(*reflect.SliceHeader)(unsafe.Pointer(&buffer)).Cap = len(str)
	return buffer
}
