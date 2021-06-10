package sql

import (
	"github.com/squareup/pranadb/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestManager(t *testing.T) {
	store := storage.NewFakeStorage()
	manager := NewManager(store)

	manager.CreateSource("test", "source1", []string{"col1", "col2"}, []ColumnType{TypeVarchar, TypeBigInt}, nil, 1, nil)

	sql := "select * from test.source1 where col2 > 32"
	err := manager.CreateMaterializedView("test", "mv1", sql, 1)
	require.Nil(t, err)
}
