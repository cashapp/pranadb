package common

import (
	"bytes"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestKeyEncodeInt64(t *testing.T) {
	vals := []int64{
		math.MinInt64,
		math.MinInt64 + 1,
		math.MinInt64 + 1000,
		-1000,
		-1,
		0,
		1,
		1000,
		math.MaxInt64 - 1000,
		math.MaxInt64 - 1,
		math.MaxInt64,
	}
	for i := 0; i < len(vals)-1; i++ {
		checkLessThan(t, encodeInt64(vals[i]), encodeInt64(vals[i+1]))
	}
}

func TestKeyEncodeFloat64(t *testing.T) {
	vals := []float64{
		-math.MaxFloat64,
		-1.234e10,
		-1e3,
		-1.1,
		-1.0,
		-0.5,
		0.0,
		0.5,
		1.0,
		1.1,
		1e3,
		1.234e10,
		math.MaxFloat64,
	}
	for i := 0; i < len(vals)-1; i++ {
		checkLessThan(t, encodeFloat64(vals[i]), encodeFloat64(vals[i+1]))
	}
}

func TestKeyEncodeDecodeFloat64(t *testing.T) {
	testKeyEncodeDecodeFloat64(t, -1234.4321)
	testKeyEncodeDecodeFloat64(t, -0.002)
	testKeyEncodeDecodeFloat64(t, 0.0034)
	testKeyEncodeDecodeFloat64(t, 4321.5436)
	testKeyEncodeDecodeFloat64(t, 0)
}

func testKeyEncodeDecodeFloat64(t *testing.T, val float64) {
	t.Helper()
	buff := KeyEncodeFloat64([]byte{}, val)
	f, _ := KeyDecodeFloat64(buff, 0)
	require.Equal(t, val, f)
}

func TestKeyEncodeString(t *testing.T) {
	vals := []string{
		"",
		"a",
		"aa",
		"aaa",
		"aaaa",
		"aab",
		"ab",
		"abb",
		"antelopes",
		"b",
		"z",
		"zzz",
	}
	for i := 0; i < len(vals)-1; i++ {
		checkLessThan(t, encodeString(vals[i]), encodeString(vals[i+1]))
	}
}

func TestKeyEncodeDecimal(t *testing.T) {
	vals := []string{
		"-1000000.1234",
		"-1000000.1233",
		"-10.00",
		"-1.1",
		"-1.0",
		"-0.5",
		"-0.4",
		"0.0",
		"0.4",
		"0.5",
		"1.0",
		"1.1",
		"10.00",
		"1000000.1233",
		"1000000.1234",
	}
	for i := 0; i < len(vals)-1; i++ {
		dec1, err := NewDecFromString(vals[i])
		require.NoError(t, err)
		dec2, err := NewDecFromString(vals[i+1])
		require.NoError(t, err)
		b1, err := encodeDecimal(*dec1, 11, 4)
		require.NoError(t, err)
		b2, err := encodeDecimal(*dec2, 11, 4)
		require.NoError(t, err)
		checkLessThan(t, b1, b2)
	}
}

func TestKeyEncodeTimestamp(t *testing.T) {
	vals := []string{
		"2021-01-01",
		"2021-01-02",
		"2021-01-02 12:34:56",
		"2021-01-02 12:34:56.789",
		"2021-01-02 12:34:56.789999",
		"2021-01-02 12:34:56.999999",
		"2021-01-02 12:34:57",
		"2021-01-02 12:35:57",
		"2021-01-02 13:35:57",
		"2021-01-03 13:35:57",
		"2021-02-03 13:35:57",
		"2022-02-03 13:35:57",
	}
	for i := 0; i < len(vals)-1; i++ {
		ts1 := NewTimestampFromString(vals[i])
		ts2 := NewTimestampFromString(vals[i+1])
		b1, err := KeyEncodeTimestamp(nil, ts1)
		require.NoError(t, err)
		b2, err := KeyEncodeTimestamp(nil, ts2)
		require.NoError(t, err)
		checkLessThan(t, b1, b2)
	}
}

func encodeInt64(val int64) []byte {
	return KeyEncodeInt64([]byte{}, val)
}

func encodeFloat64(val float64) []byte {
	return KeyEncodeFloat64([]byte{}, val)
}

func encodeString(val string) []byte {
	return KeyEncodeString([]byte{}, val)
}

func encodeDecimal(val Decimal, precision int, scale int) ([]byte, error) {
	return KeyEncodeDecimal([]byte{}, val, precision, scale)
}

func checkLessThan(t *testing.T, b1, b2 []byte) {
	t.Helper()
	diff := bytes.Compare(b1, b2)
	require.Equal(t, -1, diff, "expected %x < %x", b1, b2)
}
