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
	"testing"

	planner2 "github.com/squareup/pranadb/tidb/planner"
	"github.com/stretchr/testify/require"
)

func TestPointGetUsesTableScanWithUnitaryRangeForPullQuery(t *testing.T) {
	schema := createTestSchema()
	planner := NewPlanner(schema)
	physi, _, err := planner.QueryToPlan("select col0, col1, col2 from table1 where col0=123", false, true)
	require.NoError(t, err)
	ts, ok := physi.(*planner2.PhysicalTableScan)
	require.True(t, ok)
	require.Equal(t, 1, len(ts.Ranges))
	require.Equal(t, int64(123), ts.Ranges[0].LowVal[0].GetInt64())
	require.Equal(t, int64(123), ts.Ranges[0].HighVal[0].GetInt64())
}

func TestPointGetUsesSelectForPushQuery(t *testing.T) {
	schema := createTestSchema()
	planner := NewPlanner(schema)
	physi, _, err := planner.QueryToPlan("select col0, col1, col2 from table1 where col0=123", false, false)
	require.NoError(t, err)
	sel, ok := physi.(*planner2.PhysicalSelection)
	require.True(t, ok)
	require.Equal(t, 1, len(sel.Children()))
	ts, ok := sel.Children()[0].(*planner2.PhysicalTableScan)
	require.True(t, ok)
	require.Equal(t, 1, len(ts.Ranges))
	require.True(t, ts.Ranges[0].IsFullRange())
}

func TestPointGetUsesIndexScanForPullQuery(t *testing.T) {
	schema := createTestSchema()
	schema, err := attachIndexToSchema(schema)
	require.NoError(t, err)
	planner := NewPlanner(schema)
	physi, _, err := planner.QueryToPlan("select col2 from table1 where col2=1", false, true)
	require.NoError(t, err)
	is, ok := physi.(*planner2.PhysicalIndexScan)
	require.True(t, ok)
	require.Equal(t, 0, len(is.Children()))
	require.Equal(t, 1, len(is.Ranges))
	require.True(t, is.Ranges[0].IsPoint(planner.StatementContext()))
}

func TestRangeUsesIndexScanForPullQuery(t *testing.T) {
	schema := createTestSchema()
	schema, err := attachIndexToSchema(schema)
	require.NoError(t, err)
	planner := NewPlanner(schema)
	physi, _, err := planner.QueryToPlan("select col2 from table1 where col2 > 1", false, true)
	require.NoError(t, err)
	is, ok := physi.(*planner2.PhysicalIndexScan)
	require.True(t, ok)
	require.Equal(t, 0, len(is.Children()))
	require.Equal(t, 1, len(is.Ranges))
}
