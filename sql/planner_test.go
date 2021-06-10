package sql

import (
	"context"
	"github.com/squareup/pranadb/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestPlanner(t *testing.T) {

	pr := NewParser()

	stmtNode, err := pr.Parse("select a, b from test.table1 where b > 3")

	require.Nil(t, err)

	pl := NewPlanner()

	schemaManager, err := CreateSchemaManager()
	require.Nil(t, err)

	is, err := schemaManager.ToInfoSchema()
	require.Nil(t, err)

	ctx := context.TODO()
	sessCtx := NewSessionContext()

	logical, err := pl.CreateLogicalPlan(ctx, sessCtx, stmtNode, is)
	require.Nil(t, err)

	physical, err := pl.CreatePhysicalPlan(ctx, sessCtx, logical, true, false)
	require.Nil(t, err)

	println(physical.ExplainInfo())

}

func CreateSchemaManager() (Manager, error) {
	tableInfo := TableInfo{}
	tableInfo.
		Id(0).
		Name("table1").
		AddColumn("a", TypeVarchar).
		AddColumn("b", TypeBigInt).
		AddColumn("c", TypeBigInt)

	stor := storage.NewFakeStorage()
	schemaManager := NewManager(stor)

	colNames := []string{"a", "b", "c"}
	colTypes := []ColumnType{TypeVarchar, TypeBigInt, TypeBigInt}

	err := schemaManager.CreateSource("test", "table1", colNames, colTypes, nil, 1, nil)
	if err != nil {
		return nil, err
	}

	return schemaManager, nil
}
