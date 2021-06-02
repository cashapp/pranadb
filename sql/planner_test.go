package sql

import (
	"context"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/mock"
	"github.com/squareup/pranadb/sql/parse"
	"github.com/squareup/pranadb/sql/planner"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPlanner(t *testing.T) {

	pr := parse.NewParser()

	stmtNode, err := pr.Parse("select a,b from test.table1 where a > b")
	assert.Nil(t, err)

	pl := planner.NewPlanner()

	is := infoschema.MockInfoSchema([]*model.TableInfo{CreateTable()})

	ctx := context.TODO()
	sessCtx := mock.NewContext()

	store, err := mockstore.NewMockStore()
	assert.Nil(t, err)

	d := domain.NewDomain(store, 0, 0, 0, nil)
	domain.BindDomain(sessCtx, d)

	logical, err := pl.CreateLogicalPlan(ctx, sessCtx, stmtNode, is)
	assert.Nil(t, err)

	physical, err := pl.CreatePhysicalPlan(ctx, sessCtx, logical, true, true)
	assert.Nil(t, err)

	println(physical.ExplainInfo())

}


func CreateTable() *model.TableInfo {

	indices := []*model.IndexInfo{
		{
			Name: model.NewCIStr("c_d_e"),
			Columns: []*model.IndexColumn{
				{
					Name:   model.NewCIStr("c"),
					Length: types.UnspecifiedLength,
					Offset: 2,
				},
				{
					Name:   model.NewCIStr("d"),
					Length: types.UnspecifiedLength,
					Offset: 3,
				},
				{
					Name:   model.NewCIStr("e"),
					Length: types.UnspecifiedLength,
					Offset: 4,
				},
			},
			State:  model.StatePublic,
			Unique: true,
		},
	}

	pkColumn := &model.ColumnInfo{
		State:     model.StatePublic,
		Offset:    0,
		Name:      model.NewCIStr("a"),
		FieldType: newLongType(),
		ID:        1,
	}
	col0 := &model.ColumnInfo{
		State:     model.StatePublic,
		Offset:    1,
		Name:      model.NewCIStr("b"),
		FieldType: newLongType(),
		ID:        2,
	}
	col1 := &model.ColumnInfo{
		State:     model.StatePublic,
		Offset:    2,
		Name:      model.NewCIStr("c"),
		FieldType: newLongType(),
		ID:        3,
	}
	col2 := &model.ColumnInfo{
		State:     model.StatePublic,
		Offset:    3,
		Name:      model.NewCIStr("d"),
		FieldType: newLongType(),
		ID:        4,
	}
	col3 := &model.ColumnInfo{
		State:     model.StatePublic,
		Offset:    4,
		Name:      model.NewCIStr("e"),
		FieldType: newLongType(),
		ID:        5,
	}
	colStr1 := &model.ColumnInfo{
		State:     model.StatePublic,
		Offset:    5,
		Name:      model.NewCIStr("c_str"),
		FieldType: newStringType(),
		ID:        6,
	}
	colStr2 := &model.ColumnInfo{
		State:     model.StatePublic,
		Offset:    6,
		Name:      model.NewCIStr("d_str"),
		FieldType: newStringType(),
		ID:        7,
	}
	colStr3 := &model.ColumnInfo{
		State:     model.StatePublic,
		Offset:    7,
		Name:      model.NewCIStr("e_str"),
		FieldType: newStringType(),
		ID:        8,
	}
	col4 := &model.ColumnInfo{
		State:     model.StatePublic,
		Offset:    8,
		Name:      model.NewCIStr("f"),
		FieldType: newLongType(),
		ID:        9,
	}
	col5 := &model.ColumnInfo{
		State:     model.StatePublic,
		Offset:    9,
		Name:      model.NewCIStr("g"),
		FieldType: newLongType(),
		ID:        10,
	}
	col6 := &model.ColumnInfo{
		State:     model.StatePublic,
		Offset:    10,
		Name:      model.NewCIStr("h"),
		FieldType: newLongType(),
		ID:        11,
	}
	col7 := &model.ColumnInfo{
		State:     model.StatePublic,
		Offset:    11,
		Name:      model.NewCIStr("i_date"),
		FieldType: newDateType(),
		ID:        12,
	}
	pkColumn.Flag = mysql.PriKeyFlag | mysql.NotNullFlag
	// Column 'b', 'c', 'd', 'f', 'g' is not null.
	col0.Flag = mysql.NotNullFlag
	col1.Flag = mysql.NotNullFlag
	col2.Flag = mysql.NotNullFlag
	col4.Flag = mysql.NotNullFlag
	col5.Flag = mysql.NotNullFlag
	col6.Flag = mysql.NoDefaultValueFlag
	table := &model.TableInfo{
		Columns:    []*model.ColumnInfo{pkColumn, col0, col1, col2, col3, colStr1, colStr2, colStr3, col4, col5, col6, col7},
		Indices:    indices,
		Name:       model.NewCIStr("table1"),
		PKIsHandle: true,
	}
	return table
}

func newLongType() types.FieldType {
	return *(types.NewFieldType(mysql.TypeLong))
}

func newStringType() types.FieldType {
	ft := types.NewFieldType(mysql.TypeVarchar)
	ft.Charset, ft.Collate = types.DefaultCharsetForType(mysql.TypeVarchar)
	return *ft
}

func newDateType() types.FieldType {
	ft := types.NewFieldType(mysql.TypeDate)
	return *ft
}

