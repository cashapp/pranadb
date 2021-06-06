package planner

import (
	"context"
	"github.com/squareup/pranadb/sql"
	"github.com/squareup/pranadb/sql/parse"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestPlanner(t *testing.T) {

	pr := parse.NewParser()

	stmtNode, err := pr.Parse("select a, max(b) from test.table1 group by a")

	require.Nil(t, err)

	pl := NewPlanner()

	schemaManager := createSchemaManager()

	is, err := sql.SchemasToInfoSchema(schemaManager)
	require.Nil(t, err)

	ctx := context.TODO()
	sessCtx := NewSessionContext()

	logical, err := pl.CreateLogicalPlan(ctx, sessCtx, stmtNode, is)
	require.Nil(t, err)

	physical, err := pl.CreatePhysicalPlan(ctx, sessCtx, logical, true, false)
	require.Nil(t, err)

	println(physical.ExplainInfo())

}

func createSchemaManager() sql.SchemaManager {
	tableInfo := sql.TableInfo{}
	tableInfo.
		Id(0).
		Name("table1").
		AddColumn("a", sql.VarcharColumnType).
		AddColumn("b", sql.BigIntColumnType).
		AddColumn("c", sql.BigIntColumnType)

	schemaManager := sql.NewSchemaManager()
	schemaManager.AddTable("test", &tableInfo)
	return schemaManager
}
