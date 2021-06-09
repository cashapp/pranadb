package planner

import (
	"context"
	"github.com/pingcap/tidb/infoschema"
	"github.com/squareup/pranadb/sql/parse"
	"github.com/squareup/pranadb/sql/tidb"
	"github.com/stretchr/testify/require"
	"testing"
)

var pr parse.Parser
var pl Planner
var is infoschema.InfoSchema

func TestMain(m *testing.M) {
	pr = parse.NewParser()

	pl = NewPlanner()

	schemaManager := CreateSchemaManager()

	var err error
	is, err = tidb.SchemasToInfoSchema(schemaManager)
	if err != nil {
		println(err)
		return
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
		sessCtx := tidb.NewSessionContext()

		logical, err := pl.CreateLogicalPlan(ctx, sessCtx, stmtNode, is)
		require.Nil(b, err)
		require.NotNil(b, logical)
	}
}
