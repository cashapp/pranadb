package parse

import (
	"github.com/stretchr/testify/require"
	"testing"
)

var pr Parser

func TestMain(m *testing.M) {
	pr = NewParser()
	m.Run()
}

func BenchmarkParser(b *testing.B) {
	for i := 0; i < b.N; i++ {
		stmtNode, err := pr.Parse("select a, max(b) from test.table1 group by a")
		require.Nil(b, err)
		require.NotNil(b, stmtNode)
	}
}
