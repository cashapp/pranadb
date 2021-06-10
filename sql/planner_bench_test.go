package sql

import (
	"context"
	"github.com/pingcap/tidb/infoschema"
	"github.com/stretchr/testify/require"
	"testing"
)

var pr Parser
var pl Planner
var is infoschema.InfoSchema

func TestMain(m *testing.M) {
	pr = NewParser()

	pl = NewPlanner()

	schemaManager, err := CreateSchemaManager()
	if err != nil {
		panic(err)
	}

	is, err = schemaManager.ToInfoSchema()
	if err != nil {
		panic(err)
	}
	m.Run()
}

func BenchmarkParser(b *testing.B) {
	for i := 0; i < b.N; i++ {
		stmtNode, err := pr.Parse("select a, max(b) from test.table1 group by a")
		require.Nil(b, err)
		require.NotNil(b, stmtNode)
	}
}

func BenchmarkLogicalPlan(b *testing.B) {
	stmtNode, err := pr.Parse("select a, max(b) from test.table1 group by a")
	require.Nil(b, err)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ctx := context.TODO()
		sessCtx := NewSessionContext()

		logical, err := pl.CreateLogicalPlan(ctx, sessCtx, stmtNode, is)
		require.Nil(b, err)
		require.NotNil(b, logical)
	}
}
