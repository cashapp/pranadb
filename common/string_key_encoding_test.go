package common

import (
	"bytes"
	"github.com/stretchr/testify/require"
	"math/rand"
	"sort"
	"testing"
	"time"
)

func TestStringKeyEncodeDecode(t *testing.T) {
	strs := generateRandomStrings(100)
	for _, str := range strs {
		testStringKeyEncodeDecode(t, str)
	}
}

func testStringKeyEncodeDecode(t *testing.T, str string) {
	t.Helper()
	buff := KeyEncodeString([]byte{}, str)
	res, offset, err := KeyDecodeString(buff, 0)
	require.NoError(t, err)
	require.Equal(t, len(buff), offset)
	require.Equal(t, str, res)
}

func TestStringKeyEncodeDecodeExistingBuffer(t *testing.T) {
	strs := generateRandomStrings(100)
	for _, str := range strs {
		testStringKeyEncodeDecodeExistingBuffer(t, str)
	}
}

func testStringKeyEncodeDecodeExistingBuffer(t *testing.T, str string) {
	t.Helper()
	buff := KeyEncodeString([]byte{}, str)
	buff = append([]byte("aardvarks"), buff...)
	off := len(buff)
	buff = append(buff, []byte("antelopes")...)
	res, offset, err := KeyDecodeString(buff, 9)
	require.NoError(t, err)
	require.Equal(t, off, offset)
	require.Equal(t, "antelopes", string(buff[off:]))
	require.Equal(t, str, res)
}

func TestBinaryOrdering(t *testing.T) {
	strs := generateRandomStrings(100)

	strsCopy := make([]string, len(strs))
	copy(strsCopy, strs)

	// First sort the strings in lexographic order
	sort.Strings(strs)

	// Encode the strings into []byte
	encodedStrs := make([][]byte, len(strs))
	for i, str := range strsCopy {
		encodedStrs[i] = KeyEncodeString([]byte{}, str)
	}

	// Then sort them in binary order
	sort.Slice(encodedStrs, func(i, j int) bool { return bytes.Compare(encodedStrs[i], encodedStrs[j]) < 0 })

	// Now decode them
	decodedStrs := make([]string, len(strs))
	for i, b := range encodedStrs {
		var err error
		decodedStrs[i], _, err = KeyDecodeString(b, 0)
		require.NoError(t, err)
	}

	// They should be in the same order as the lexographically sorted strings
	for i := 0; i < len(strs); i++ {
		require.Equal(t, strs[i], decodedStrs[i])
	}
}

func generateRandomStrings(upToLen int) []string {
	rand.Seed(time.Now().UnixNano())
	res := make([]string, upToLen)
	for i := 0; i < upToLen; i++ {
		res[i] = generateRandomString(i)
	}
	return res
}

var alpha = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func generateRandomString(length int) string {
	bytes := make([]rune, length)
	for i := 0; i < length; i++ {
		bytes[i] = alpha[rand.Intn(len(alpha))]
	}
	return string(bytes)
}
