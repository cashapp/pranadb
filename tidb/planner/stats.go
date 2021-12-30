//
// This source code is a modified form of original source from the TiDB project, which has the following copyright header(s):
//

// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package planner

import (
	"github.com/squareup/pranadb/tidb/expression"
	"github.com/squareup/pranadb/tidb/planner/property"
	"github.com/squareup/pranadb/tidb/util/logutil"
	"go.uber.org/zap"
	"math"
)

func (p *basePhysicalPlan) StatsCount() float64 {
	return p.stats.RowCount
}

func deriveLimitStats(childProfile *property.StatsInfo, limitCount float64) *property.StatsInfo {
	stats := &property.StatsInfo{
		RowCount: math.Min(limitCount, childProfile.RowCount),
		ColNDVs:  make(map[int64]float64, len(childProfile.ColNDVs)),
	}
	for id, c := range childProfile.ColNDVs {
		stats.ColNDVs[id] = math.Min(c, stats.RowCount)
	}
	return stats
}

func getGroupNDV4Cols(cols []*expression.Column, stats *property.StatsInfo) *property.GroupNDV {
	if len(cols) == 0 || len(stats.GroupNDVs) == 0 {
		return nil
	}
	cols = expression.SortColumns(cols)
	for _, groupNDV := range stats.GroupNDVs {
		if len(cols) != len(groupNDV.Cols) {
			continue
		}
		match := true
		for i, col := range groupNDV.Cols {
			if col != cols[i].UniqueID {
				match = false
				break
			}
		}
		if match {
			return &groupNDV
		}
	}
	return nil
}

// getColsNDV returns the NDV of a couple of columns.
// If the columns match any GroupNDV maintained by child operator, we can get an accurate NDV.
// Otherwise, we simply return the max NDV among the columns, which is a lower bound.
func getColsNDV(cols []*expression.Column, schema *expression.Schema, profile *property.StatsInfo) float64 {
	NDV := 1.0
	if groupNDV := getGroupNDV4Cols(cols, profile); groupNDV != nil {
		return math.Max(groupNDV.NDV, NDV)
	}
	indices := schema.ColumnsIndices(cols)
	if indices == nil {
		logutil.BgLogger().Error("column not found in schema", zap.Any("columns", cols), zap.String("schema", schema.String()))
		return NDV
	}
	for _, idx := range indices {
		// It is a very naive estimation.
		col := schema.Columns[idx]
		NDV = math.Max(NDV, profile.ColNDVs[col.UniqueID])
	}
	return NDV
}

type fullJoinRowCountHelper struct {
	cartesian     bool
	leftProfile   *property.StatsInfo
	rightProfile  *property.StatsInfo
	leftJoinKeys  []*expression.Column
	rightJoinKeys []*expression.Column
	leftSchema    *expression.Schema
	rightSchema   *expression.Schema
}

func (h *fullJoinRowCountHelper) estimate() float64 {
	if h.cartesian {
		return h.leftProfile.RowCount * h.rightProfile.RowCount
	}
	leftKeyNDV := getColsNDV(h.leftJoinKeys, h.leftSchema, h.leftProfile)
	rightKeyNDV := getColsNDV(h.rightJoinKeys, h.rightSchema, h.rightProfile)
	count := h.leftProfile.RowCount * h.rightProfile.RowCount / math.Max(leftKeyNDV, rightKeyNDV)
	return count
}
