package common

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestDecimalFromString(t *testing.T) {
	dec, err := NewDecFromString("12345678.87654321")
	require.NoError(t, err)
	require.Equal(t, "12345678.87654321", dec.ToString())
}

func TestDecimalFromFloat64(t *testing.T) {
	dec, err := NewDecFromFloat64(1.23e10)
	require.NoError(t, err)
	require.Equal(t, "12300000000", dec.ToString())
}

func TestEncodeDecode(t *testing.T) {
	dec, err := NewDecFromString("12345678.87654321")
	require.NoError(t, err)
	var buff []byte
	buff, err = dec.Encode(buff, 16, 8)
	require.NoError(t, err)

	dec2 := Decimal{}
	off, err := dec2.Decode(buff, 0, 16, 8)
	require.NoError(t, err)
	require.Equal(t, len(buff), off)

	require.Equal(t, "12345678.87654321", dec2.ToString())
}
