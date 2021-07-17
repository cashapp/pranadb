package commontest

import (
	"github.com/squareup/pranadb/common"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestDecimalFromString(t *testing.T) {
	dec, err := common.NewDecFromString("12345678.87654321")
	require.NoError(t, err)
	require.Equal(t, "12345678.87654321", dec.String())
}

func TestDecimalFromFloat64(t *testing.T) {
	dec, err := common.NewDecFromFloat64(1.23e10)
	require.NoError(t, err)
	require.Equal(t, "12300000000", dec.String())
}

func TestEncodeDecode(t *testing.T) {
	dec, err := common.NewDecFromString("12345678.87654321")
	require.NoError(t, err)
	var buff []byte
	buff, err = dec.Encode(buff, 16, 8)
	require.NoError(t, err)

	dec2 := common.Decimal{}
	off, err := dec2.Decode(buff, 0, 16, 8)
	require.NoError(t, err)
	require.Equal(t, len(buff), off)

	require.Equal(t, "12345678.87654321", dec2.String())
}
