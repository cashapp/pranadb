/*
 *  Copyright 2022 Square Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package parplan

import (
	"testing"

	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/errors"
	"github.com/stretchr/testify/require"
)

func TestSimpleWildcard(t *testing.T) {
	testLogicalPlan(t, "select * from table1",
		`Projection
Schema: Columns: [test.table1.col0,test.table1.col1,test.table1.col2]
Expressions: [test.table1.col0,test.table1.col1,test.table1.col2]
|
|
|
v
DataSource
Schema: Columns: [test.table1.col0,test.table1.col1,test.table1.col2]
`)
}

func TestSingleColumn(t *testing.T) {
	testLogicalPlan(t, "select col0 from table1",
		`Projection
Schema: Columns: [test.table1.col0]
Expressions: [test.table1.col0]
|
|
|
v
DataSource
Schema: Columns: [test.table1.col0,test.table1.col1,test.table1.col2]
`)
}

func TestMultipleColumns(t *testing.T) {
	testLogicalPlan(t, "select col0, col1, col2 from table1",
		`Projection
Schema: Columns: [test.table1.col0,test.table1.col1,test.table1.col2]
Expressions: [test.table1.col0,test.table1.col1,test.table1.col2]
|
|
|
v
DataSource
Schema: Columns: [test.table1.col0,test.table1.col1,test.table1.col2]
`)
}

func TestSimpleWhere(t *testing.T) {
	testLogicalPlan(t, "select col0 from table1 where col0=12345",
		`Projection
Schema: Columns: [test.table1.col0]
Expressions: [test.table1.col0]
|
|
|
v
Selection:
Conditions: [eq(test.table1.col0, 12345)]
|
|
|
v
DataSource
Schema: Columns: [test.table1.col0,test.table1.col1,test.table1.col2]
`)
}

func TestSimpleAggregation(t *testing.T) {
	testLogicalPlan(t, "select col0, count(col1) from table1 group by col0",
		`Projection
Schema: Columns: [test.table1.col0,Column#4]
Expressions: [test.table1.col0,Column#4]
|
|
|
v
Aggregation
Schema: Columns: [Column#4,test.table1.col0,test.table1.col1,test.table1.col2]
Aggregate functions: [count(test.table1.col1),firstrow(test.table1.col0),firstrow(test.table1.col1),firstrow(test.table1.col2)]
Group-by items: [test.table1.col0]
|
|
|
v
DataSource
Schema: Columns: [test.table1.col0,test.table1.col1,test.table1.col2]
`)
}

func TestUnionAll(t *testing.T) {
	testLogicalPlan(t, "select * from table1 union all select * from table2",
		`UnionAll
Schema: Columns: [Column#7,Column#8,Column#9]
|
|
|
v
============ Child 0
Projection
Schema: Columns: [Column#7,Column#8,Column#9]
Expressions: [test.table1.col0,test.table1.col1,test.table1.col2]
|
|
|
v
Projection
Schema: Columns: [test.table1.col0,test.table1.col1,test.table1.col2]
Expressions: [test.table1.col0,test.table1.col1,test.table1.col2]
|
|
|
v
DataSource
Schema: Columns: [test.table1.col0,test.table1.col1,test.table1.col2]
============ Child 1
Projection
Schema: Columns: [Column#7,Column#8,Column#9]
Expressions: [test.table2.col0,test.table2.col1,test.table2.col2]
|
|
|
v
Projection
Schema: Columns: [test.table2.col0,test.table2.col1,test.table2.col2]
Expressions: [test.table2.col0,test.table2.col1,test.table2.col2]
|
|
|
v
DataSource
Schema: Columns: [test.table2.col0,test.table2.col1,test.table2.col2]
`)
}

func TestOrderBy(t *testing.T) {
	testLogicalPlan(t, "select col0, col1, col2 from table1 order by col0, col1",
		`Sort:
By-Items: test.table1.col0test.table1.col1
|
|
|
v
Projection
Schema: Columns: [test.table1.col0,test.table1.col1,test.table1.col2]
Expressions: [test.table1.col0,test.table1.col1,test.table1.col2]
|
|
|
v
DataSource
Schema: Columns: [test.table1.col0,test.table1.col1,test.table1.col2]
`)
}

func testLogicalPlan(t *testing.T, query string, expectedPlan string) {
	t.Helper()
	schema := createTestSchema()
	planner := NewPlanner(schema)
	ast, _, err := planner.parser.Parse(query)
	require.NoError(t, err)
	err = planner.preprocess(ast.stmt, false)
	require.NoError(t, err)
	logicalPlan, err := planner.BuildLogicalPlan(ast, false)
	require.NoError(t, err)
	planString := logicalPlan.Dump()
	println(planString)
	require.Equal(t, expectedPlan, planString)
}

func createTestSchema() *common.Schema {
	schema := common.NewSchema("test")
	table1 := &common.TableInfo{
		ID:             0,
		SchemaName:     "test",
		Name:           "table1",
		PrimaryKeyCols: []int{0},
		ColumnNames:    []string{"col0", "col1", "col2"},
		ColumnTypes:    []common.ColumnType{common.BigIntColumnType, common.VarcharColumnType, common.IntColumnType},
		IndexInfos:     nil,
		ColsVisible:    nil,
		Internal:       false,
	}
	schema.PutTable(table1.Name, table1)
	table2 := &common.TableInfo{
		ID:             0,
		SchemaName:     "test",
		Name:           "table2",
		PrimaryKeyCols: []int{0},
		ColumnNames:    []string{"col0", "col1", "col2"},
		ColumnTypes:    []common.ColumnType{common.BigIntColumnType, common.VarcharColumnType, common.IntColumnType},
		IndexInfos:     nil,
		ColsVisible:    nil,
		Internal:       false,
	}
	schema.PutTable(table2.Name, table2)
	table3 := &common.TableInfo{
		ID:             0,
		SchemaName:     "test",
		Name:           "table3",
		PrimaryKeyCols: []int{0, 1, 2},
		ColumnNames:    []string{"col0", "col1", "col2", "col3", "col4"},
		ColumnTypes:    []common.ColumnType{common.BigIntColumnType, common.VarcharColumnType, common.IntColumnType, common.VarcharColumnType, common.BigIntColumnType},
		IndexInfos:     nil,
		ColsVisible:    nil,
		Internal:       false,
	}
	schema.PutTable(table3.Name, table3)
	table4 := &common.TableInfo{
		ID:             0,
		SchemaName:     "test",
		Name:           "table4",
		PrimaryKeyCols: []int{0},
		ColumnNames:    []string{"col0", "col1", "col2", "col3", "col4"},
		ColumnTypes:    []common.ColumnType{common.BigIntColumnType, common.VarcharColumnType, common.IntColumnType, common.VarcharColumnType, common.BigIntColumnType},
		IndexInfos:     nil,
		ColsVisible:    nil,
		Internal:       false,
	}
	schema.PutTable(table4.Name, table4)
	return schema
}

func attachIndexToSchema(schema *common.Schema) (*common.Schema, error) {
	index1 := &common.IndexInfo{
		ID:         0,
		SchemaName: "test",
		Name:       "index1",
		TableName:  "table1",
		IndexCols:  []int{2},
	}
	if err := schema.PutIndex(index1); err != nil {
		return nil, errors.WithStack(err)
	}
	return schema, nil
}

func attachMultiColumnIndexToSchema(schema *common.Schema) (*common.Schema, error) {
	index1 := &common.IndexInfo{
		ID:         0,
		SchemaName: "test",
		Name:       "index1",
		TableName:  "table4",
		IndexCols:  []int{1, 2, 3},
	}
	if err := schema.PutIndex(index1); err != nil {
		return nil, errors.WithStack(err)
	}
	return schema, nil
}
