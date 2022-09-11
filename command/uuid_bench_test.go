package command

import (
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"testing"
)

func BenchmarkUUID(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, err := uuid.NewRandom()
		require.NoError(b, err)
	}
}
