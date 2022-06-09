package commontest

import (
	"testing"

	"github.com/squareup/pranadb/common"
	"github.com/stretchr/testify/require"
)

func TestByteSliceMap(t *testing.T) {
	bsl := common.NewByteSliceMap()
	k := []byte("somekey")
	v := []byte("somevalue")
	bsl.Put(k, v)

	v2, ok := bsl.Get(k)

	require.True(t, ok)
	require.Equal(t, "somevalue", string(v2))

	_, ok = bsl.Get([]byte("not_exists"))
	require.False(t, ok)
}

func TestByteSliceToStringZeroCopy(t *testing.T) {
	b1 := []byte("string1")
	b2 := []byte("")

	s1 := common.ByteSliceToStringZeroCopy(b1)
	require.Equal(t, "string1", s1)
	s2 := common.ByteSliceToStringZeroCopy(b2)
	require.Equal(t, "", s2)
}

func TestStringToByteSliceZeroCopy(t *testing.T) {
	s1 := "string1"
	s2 := ""

	b1 := common.StringToByteSliceZeroCopy(s1)
	require.Equal(t, "string1", string(b1))
	b2 := common.StringToByteSliceZeroCopy(s2)
	require.Equal(t, "", string(b2))
}

func TestIncrementBytesBigEndian(t *testing.T) {
	incAndCheckBytes(t, []byte{0, 0, 0, 0}, []byte{0, 0, 0, 1})
	incAndCheckBytes(t, []byte{0, 0, 0, 1}, []byte{0, 0, 0, 2})
	incAndCheckBytes(t, []byte{0, 0, 0, 254}, []byte{0, 0, 0, 255})
	incAndCheckBytes(t, []byte{0, 0, 0, 255}, []byte{0, 0, 1, 0})
	incAndCheckBytes(t, []byte{0, 0, 1, 0}, []byte{0, 0, 1, 1})
	incAndCheckBytes(t, []byte{0, 0, 1, 1}, []byte{0, 0, 1, 2})
	incAndCheckBytes(t, []byte{0, 0, 1, 254}, []byte{0, 0, 1, 255})
	incAndCheckBytes(t, []byte{0, 0, 1, 255}, []byte{0, 0, 2, 0})
	incAndCheckBytes(t, []byte{0, 0, 2, 0}, []byte{0, 0, 2, 1})
	incAndCheckBytes(t, []byte{255, 255, 255, 254}, []byte{255, 255, 255, 255})
	defer func() {
		err := recover()
		if err != nil {
			require.Equal(t, "cannot increment key - all bits set", err)
		} else {
			// expected a panic
			t.Fail()
		}
	}()
	incAndCheckBytes(t, []byte{255, 255, 255, 255}, []byte{255, 255, 255, 255})
}

func incAndCheckBytes(t *testing.T, bytes []byte, expected []byte) {
	t.Helper()
	res := common.IncrementBytesBigEndian(bytes)
	require.Equal(t, expected, res)
}
