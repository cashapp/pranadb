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
 */

package parplan

import (
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/tidb/util/ranger"
	"testing"

	planner2 "github.com/squareup/pranadb/tidb/planner"
	"github.com/stretchr/testify/require"
)

type rng struct {
	lowVal   []interface{}
	highVal  []interface{}
	lowExcl  bool
	highExcl bool
}

func TestTableOrIndexScan(t *testing.T) {
	testQueryUsesTableOrIndexScan(t, "select * from table1", true, []*rng{nil}, []int{0}, nil)
	testQueryUsesTableOrIndexScan(t, "select * from table1 where col0=100", true,
		[]*rng{
			{
				lowVal:   []interface{}{int64(100)},
				highVal:  []interface{}{int64(100)},
				lowExcl:  false,
				highExcl: false,
			},
		},
		[]int{0}, nil)
	testQueryUsesTableOrIndexScan(t, "select * from table1 where col0=100 OR col0=200", true,
		[]*rng{
			{
				lowVal:   []interface{}{int64(100)},
				highVal:  []interface{}{int64(100)},
				lowExcl:  false,
				highExcl: false,
			},
			{
				lowVal:   []interface{}{int64(200)},
				highVal:  []interface{}{int64(200)},
				lowExcl:  false,
				highExcl: false,
			},
		},
		[]int{0}, nil)
	testQueryUsesTableOrIndexScan(t, "select * from table1 where col0 in(100, 200)", true,
		[]*rng{
			{
				lowVal:   []interface{}{int64(100)},
				highVal:  []interface{}{int64(100)},
				lowExcl:  false,
				highExcl: false,
			},
			{
				lowVal:   []interface{}{int64(200)},
				highVal:  []interface{}{int64(200)},
				lowExcl:  false,
				highExcl: false,
			},
		},
		[]int{0}, nil)
	testQueryUsesTableOrIndexScan(t, "select * from table1 where col0 > 100 and col0 < 200", true,
		[]*rng{
			{
				lowVal:   []interface{}{int64(100)},
				highVal:  []interface{}{int64(200)},
				lowExcl:  true,
				highExcl: true,
			},
		},
		[]int{0}, nil)
	testQueryUsesTableOrIndexScan(t, "select * from table1 where col0 >= 100 and col0 <= 200", true,
		[]*rng{
			{
				lowVal:   []interface{}{int64(100)},
				highVal:  []interface{}{int64(200)},
				lowExcl:  false,
				highExcl: false,
			},
		},
		[]int{0}, nil)
	// This should use an *index scan*. TiDB doesn't support multi column PKs and generating multi-column table scans
	// so we create a fake index for the PK in the case of more than one PK column, and then convert the resulting
	// index scan back into a Prana table scan in our code
	testQueryUsesTableOrIndexScan(t, "select * from table1 where col0 = 100 and col1 = 150", false,
		[]*rng{
			{
				lowVal:   []interface{}{int64(100), int64(150)},
				highVal:  []interface{}{int64(100), int64(150)},
				lowExcl:  false,
				highExcl: false,
			},
		},
		[]int{0, 1}, nil)
	testQueryUsesTableOrIndexScan(t, "select * from table1 where col1 = 150", false,
		[]*rng{
			{
				lowVal:   []interface{}{int64(150)},
				highVal:  []interface{}{int64(150)},
				lowExcl:  false,
				highExcl: false,
			},
		},
		[]int{0}, []int{1})
	testQueryUsesTableOrIndexScan(t, "select * from table1 where col1 is null", false,
		[]*rng{
			{
				lowVal:   []interface{}{nil},
				highVal:  []interface{}{nil},
				lowExcl:  false,
				highExcl: false,
			},
		},
		[]int{0}, []int{1})
	testQueryUsesTableOrIndexScan(t, "select * from table1 where col1 = 1000 and col2=10000", false,
		[]*rng{
			{
				lowVal:   []interface{}{int64(1000), int64(10000)},
				highVal:  []interface{}{int64(1000), int64(10000)},
				lowExcl:  false,
				highExcl: false,
			},
		},
		[]int{0}, []int{1, 2})
	testQueryUsesTableOrIndexScan(t, "select * from table1 where col1 = 1000 and col2>10000", false,
		[]*rng{
			{
				lowVal:   []interface{}{int64(1000), int64(10000)},
				highVal:  []interface{}{int64(1000), nil},
				lowExcl:  true,
				highExcl: false,
			},
		},
		[]int{0}, []int{1, 2})
}

func testQueryUsesTableOrIndexScan(t *testing.T, query string, tableScan bool, expectedRanges []*rng, pkCols []int, indexCols []int) {
	t.Helper()
	schema := common.NewSchema("test")
	decColumnType := common.ColumnType{
		Type:         common.TypeDecimal,
		DecPrecision: 10,
		DecScale:     2,
	}
	table := &common.TableInfo{
		ID:             0,
		SchemaName:     schema.Name,
		Name:           "table1",
		PrimaryKeyCols: pkCols,
		ColumnNames:    []string{"col0", "col1", "col2", "col3", "col4", "col5", "col6"},
		ColumnTypes: []common.ColumnType{common.TinyIntColumnType, common.IntColumnType, common.BigIntColumnType, common.DoubleColumnType,
			decColumnType, common.VarcharColumnType, common.TimestampColumnType},
	}
	schema.PutTable(table.Name, table)
	if len(indexCols) > 0 {
		indexInfo := &common.IndexInfo{
			SchemaName: schema.Name,
			ID:         0,
			TableName:  "table1",
			Name:       "index1",
			IndexCols:  indexCols,
		}
		table.IndexInfos = map[string]*common.IndexInfo{
			indexInfo.Name: indexInfo,
		}
	}
	planner := NewPlanner(schema)
	physi, _, _, err := planner.QueryToPlan(query, false, true)
	require.NoError(t, err)
	var ranges []*ranger.Range
	if tableScan {
		ts, ok := physi.(*planner2.PhysicalTableScan)
		require.True(t, ok, "should be a table scan, but is a %v", physi)
		ranges = ts.Ranges
	} else {
		is, ok := physi.(*planner2.PhysicalIndexScan)
		require.True(t, ok, "should be an index scan, but is a %v", physi)
		ranges = is.Ranges
	}
	if expectedRanges == nil {
		require.Nil(t, ranges, "ranges should be nil")
	} else {
		require.Equal(t, len(expectedRanges), len(ranges), "ranges length expected %d actual %d", len(expectedRanges), len(ranges))
		for i, rng := range ranges {
			expectedRange := expectedRanges[i]
			if expectedRange == nil {
				require.True(t, rng.IsFullRange())
			} else {
				require.Equal(t, len(expectedRange.lowVal), len(rng.LowVal), "range lowval length expected %d actual %d", len(expectedRange.lowVal), len(rng.LowVal))
				require.Equal(t, len(expectedRange.highVal), len(rng.HighVal), "range highval length expected %d actual %d", len(expectedRange.highVal), len(rng.HighVal))
				require.Equal(t, expectedRange.lowExcl, rng.LowExclude)
				require.Equal(t, expectedRange.highExcl, rng.HighExclude)
				for j, expectedLowValElem := range expectedRange.lowVal {
					expectedHighValElem := expectedRange.highVal[j]
					actualLowValElem := rng.LowVal[j].GetValue()
					actualHighValElem := rng.HighVal[j].GetValue()
					require.Equal(t, expectedLowValElem, actualLowValElem)
					require.Equal(t, expectedHighValElem, actualHighValElem)
				}
			}
		}
	}
}

func TestPointGetUsesSelectForPushQuery(t *testing.T) {
	schema := createTestSchema()
	planner := NewPlanner(schema)
	physi, _, _, err := planner.QueryToPlan("select col0, col1, col2 from table1 where col0=123", false, false)
	require.NoError(t, err)
	sel, ok := physi.(*planner2.PhysicalSelection)
	require.True(t, ok)
	require.Equal(t, 1, len(sel.Children()))
	ts, ok := sel.Children()[0].(*planner2.PhysicalTableScan)
	require.True(t, ok)
	require.Equal(t, 1, len(ts.Ranges))
	require.True(t, ts.Ranges[0].IsFullRange())
}

func TestSecondaryIndexLookupUsingIndexScanForPullQuery(t *testing.T) {
	schema := createTestSchema()
	schema, err := attachIndexToSchema(schema)
	require.NoError(t, err)
	planner := NewPlanner(schema)
	physi, _, _, err := planner.QueryToPlan("select col2 from table1 where col2=1", false, true)
	require.NoError(t, err)
	is, ok := physi.(*planner2.PhysicalIndexScan)
	require.True(t, ok)
	require.Equal(t, 0, len(is.Children()))
	require.Equal(t, 1, len(is.Ranges))
	require.True(t, is.Ranges[0].IsPoint(planner.StatementContext()))
}

func TestSecondaryIndexLookupWithInUsingIndexScanWithMultipleRangesForPullQuery(t *testing.T) {
	schema := createTestSchema()
	schema, err := attachIndexToSchema(schema)
	require.NoError(t, err)
	planner := NewPlanner(schema)
	physi, _, _, err := planner.QueryToPlan("select col2 from table1 where col2 in (100, 200, 300)", false, true)
	require.NoError(t, err)
	is, ok := physi.(*planner2.PhysicalIndexScan)
	require.True(t, ok)
	require.Equal(t, 0, len(is.Children()))
	require.Equal(t, 3, len(is.Ranges))
	require.True(t, is.Ranges[0].IsPoint(planner.StatementContext()))
	require.True(t, is.Ranges[1].IsPoint(planner.StatementContext()))
	require.True(t, is.Ranges[2].IsPoint(planner.StatementContext()))
}

func TestPrimaryKeyLookupWithInUsingTableScanWithMultipleRangesForPullQuery(t *testing.T) {
	schema := createTestSchema()
	planner := NewPlanner(schema)
	physi, _, _, err := planner.QueryToPlan("select col0 from table1 where col0 in (100, 200, 300)", false, true)
	require.NoError(t, err)
	ts, ok := physi.(*planner2.PhysicalTableScan)
	require.True(t, ok)
	require.Equal(t, 0, len(ts.Children()))
	require.Equal(t, 3, len(ts.Ranges))
	require.True(t, ts.Ranges[0].IsPoint(planner.StatementContext()))
	require.True(t, ts.Ranges[1].IsPoint(planner.StatementContext()))
	require.True(t, ts.Ranges[2].IsPoint(planner.StatementContext()))
}

func TestSecondaryIndexMultiColumnLookupUsingIndexScanForPullQuery(t *testing.T) {
	schema := createTestSchema()
	schema, err := attachMultiColumnIndexToSchema(schema)
	require.NoError(t, err)
	planner := NewPlanner(schema)
	physi, _, _, err := planner.QueryToPlan("select * from table4 where col1='foo' and col2=432 and col3='bar'", false, true)
	require.NoError(t, err)
	is, ok := physi.(*planner2.PhysicalIndexScan)
	require.True(t, ok)
	require.Equal(t, 1, len(is.Ranges))
	require.Equal(t, 3, len(is.Ranges[0].LowVal))
	require.Equal(t, "foo", is.Ranges[0].LowVal[0].GetString())
	require.Equal(t, "foo", is.Ranges[0].HighVal[0].GetString())
	require.Equal(t, int64(432), is.Ranges[0].LowVal[1].GetInt64())
	require.Equal(t, int64(432), is.Ranges[0].HighVal[1].GetInt64())
	require.Equal(t, "bar", is.Ranges[0].LowVal[2].GetString())
	require.Equal(t, "bar", is.Ranges[0].HighVal[2].GetString())
}

func TestSecondaryIndexRangeUsingIndexScanForPullQuery(t *testing.T) {
	schema := createTestSchema()
	schema, err := attachIndexToSchema(schema)
	require.NoError(t, err)
	planner := NewPlanner(schema)
	physi, _, _, err := planner.QueryToPlan("select col2 from table1 where col2 > 1", false, true)
	require.NoError(t, err)
	is, ok := physi.(*planner2.PhysicalIndexScan)
	require.True(t, ok)
	require.Equal(t, 0, len(is.Children()))
	require.Equal(t, 1, len(is.Ranges))
}

func TestSecondaryIndexMultiColumnRangeUsingIndexScanForPullQuery(t *testing.T) {
	schema := createTestSchema()
	schema, err := attachMultiColumnIndexToSchema(schema)
	require.NoError(t, err)
	planner := NewPlanner(schema)
	physi, _, _, err := planner.QueryToPlan("select * from table4 where col1='foo' and col2=432 and col3>'bar'", false, true)
	require.NoError(t, err)
	is, ok := physi.(*planner2.PhysicalIndexScan)
	require.True(t, ok)
	require.Equal(t, 1, len(is.Ranges))
	require.Equal(t, 3, len(is.Ranges[0].LowVal))
	require.Equal(t, "foo", is.Ranges[0].LowVal[0].GetString())
	require.Equal(t, "foo", is.Ranges[0].HighVal[0].GetString())
	require.Equal(t, int64(432), is.Ranges[0].LowVal[1].GetInt64())
	require.Equal(t, int64(432), is.Ranges[0].HighVal[1].GetInt64())
	require.Equal(t, "bar", is.Ranges[0].LowVal[2].GetString())
	require.Equal(t, "", is.Ranges[0].HighVal[2].GetString())
}
