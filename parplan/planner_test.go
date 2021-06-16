package parplan

import (
	"context"
	"github.com/squareup/pranadb"
	"github.com/squareup/pranadb/common"
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

func CreateSchemaManager() (pranadb.Manager, error) {
	tableInfo := common.TableInfo{}
	tableInfo.
		Id(0).
		Name("table1").
		AddColumn("a", common.TypeVarchar).
		AddColumn("b", common.TypeBigInt).
		AddColumn("c", common.TypeBigInt)

	stor := storage.NewFakeStorage()
	schemaManager := pranadb.NewManager(stor)

	colNames := []string{"a", "b", "c"}
	colTypes := []common.ColumnType{common.TypeVarchar, common.TypeBigInt, common.TypeBigInt}

	err := schemaManager.CreateSource("test", "table1", colNames, colTypes, nil, 1, nil)
	if err != nil {
		return nil, err
	}

	return schemaManager, nil
}
