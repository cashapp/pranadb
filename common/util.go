package common

import (
	"fmt"
	"io"
	"reflect"
	"runtime"
	"strings"
	"sync/atomic"
	"time"
	"unsafe"

	log "github.com/sirupsen/logrus"
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

// strings to check for to filter out goroutines when dumping stacks - Pebble for example has many goroutines that are not usually relevant
// and create a lot of clutter in the stack reports
var spamLines = []string{"releaseLoop.func1", "sync.runtime_notifyListWait"}

// DumpStacks dumps stacks for all goroutines to stdout, useful when debugging
func DumpStacks() {
	doDumpStacks(true)
}

func doDumpStacks(filterSpam bool) {
	buf := make([]byte, 1<<16)
	runtime.Stack(buf, true)
	s := string(buf)
	lines := strings.Split(s, "\n")
	ignoring := false
	for i, line := range lines {
		// Screen out Pebble spam
		if filterSpam && strings.HasPrefix(line, "goroutine ") {
			nextLine := lines[i+1]
			ignoring = false
			for _, spam := range spamLines {
				if strings.Index(nextLine, spam) != -1 {
					ignoring = true
				}
			}
		}
		if !ignoring {
			fmt.Println(line)
			// Sadly, the logging system mangles stack traces, because it orders lines by time and many lines can
			// be written with the same time, so they end up being randomly ordered. So, we introduce a sleep
			time.Sleep(1 * time.Millisecond)
		}
	}
}

func InvokeCloser(closer io.Closer) {
	if closer != nil {
		err := closer.Close()
		if err != nil {
			log.Errorf("failed to close closer %+v", err)
		}
	}
}

// IncrementBytesBigEndian returns a new byte slice which is 1 larger than the provided slice when represented in
// big endian layout, but without changing the key length
func IncrementBytesBigEndian(bytes []byte) []byte {
	inced := CopyByteSlice(bytes)
	lb := len(bytes)
	for i := lb - 1; i >= 0; i-- {
		b := bytes[i]
		if b < 255 {
			inced[i] = b + 1
			break
		}
		if i == 0 {
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
	shardID, _ := ReadUint64FromBufferBE(bytes, 0)
	//Next 8 bytes is table ID
	tableID, _ := ReadUint64FromBufferBE(bytes, 8)
	//The rest depends on the table
	remaining := bytes[16:]
	return fmt.Sprintf("sid:%05d|tid:%05d|k:%v", shardID, tableID, remaining)
}

const atFalse = 0
const atTrue = 1

type AtomicBool struct {
	val int32
}

func (a *AtomicBool) Get() bool {
	i := atomic.LoadInt32(&a.val)
	return i == atTrue
}

func (a *AtomicBool) Set(val bool) {
	atomic.StoreInt32(&a.val, a.toInt(val))
}

func (a *AtomicBool) toInt(val bool) int32 {
	// Uggghhh, why doesn't golang have an immediate if construct?
	var i int32
	if val {
		i = atTrue
	} else {
		i = atFalse
	}
	return i
}

func (a *AtomicBool) CompareAndSet(expected bool, val bool) bool {
	return atomic.CompareAndSwapInt32(&a.val, a.toInt(expected), a.toInt(val))
}
