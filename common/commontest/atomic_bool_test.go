package commontest

import (
	"testing"

	"github.com/squareup/pranadb/common"
	"github.com/stretchr/testify/require"
)

func TestAtomicBoolSetGet(t *testing.T) {
	ab := common.AtomicBool{}
	require.False(t, ab.Get())
	ab.Set(true)
	require.True(t, ab.Get())
	ab.Set(false)
	require.False(t, ab.Get())
}

// Hard to test the concurrent aspect but we can test the basics
func TestAtomicBoolCompareAndSet(t *testing.T) {
	ab := common.AtomicBool{}
	require.False(t, ab.CompareAndSet(true, false))
	require.False(t, ab.Get())

	require.True(t, ab.CompareAndSet(false, true))
	require.True(t, ab.Get())

	require.True(t, ab.CompareAndSet(true, false))
	require.False(t, ab.Get())
}
