//
// This source code is a modified form of original source from the TiDB project, which has the following copyright header(s):
//

// Copyright 2016 PingCAP, Inc.
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
	"github.com/squareup/pranadb/tidb/planner/util"
	"github.com/squareup/pranadb/tidb/sessionctx"
	"github.com/squareup/pranadb/tidb/util/plancodec"
)

// LogicalTopN represents a top-n plan.
type LogicalTopN struct {
	baseLogicalPlan

	ByItems []*util.ByItems
	Offset  uint64
	Count   uint64
}

// Init initializes LogicalTopN.
func (lt LogicalTopN) Init(ctx sessionctx.Context, offset int) *LogicalTopN {
	lt.baseLogicalPlan = newBaseLogicalPlan(ctx, plancodec.TypeTopN, &lt, offset)
	return &lt
}

// isLimit checks if TopN is a limit plan.
func (lt *LogicalTopN) isLimit() bool {
	return len(lt.ByItems) == 0
}

// BuildKeyInfo implements LogicalPlan BuildKeyInfo interface.
func (p *LogicalTopN) BuildKeyInfo(selfSchema *expression.Schema, childSchema []*expression.Schema) {
	p.baseLogicalPlan.BuildKeyInfo(selfSchema, childSchema)
	if p.Count == 1 {
		p.maxOneRow = true
	}
}

// PreparePossibleProperties implements LogicalPlan PreparePossibleProperties interface.
func (p *LogicalTopN) PreparePossibleProperties(schema *expression.Schema, childrenProperties ...[][]*expression.Column) [][]*expression.Column {
	propCols := getPossiblePropertyFromByItems(p.ByItems)
	if len(propCols) == 0 {
		return nil
	}
	return [][]*expression.Column{propCols}
}

// PruneColumns implements LogicalPlan interface.
// If any expression can view as a constant in execution stage, such as correlated column, constant,
// we do prune them. Note that we can't prune the expressions contain non-deterministic functions, such as rand().
func (lt *LogicalTopN) PruneColumns(parentUsedCols []*expression.Column) error {
	child := lt.children[0]
	var cols []*expression.Column
	lt.ByItems, cols = pruneByItems(lt.ByItems)
	parentUsedCols = append(parentUsedCols, cols...)
	return child.PruneColumns(parentUsedCols)
}

func (lt *LogicalTopN) replaceExprColumns(replace map[string]*expression.Column) {
	for _, byItem := range lt.ByItems {
		resolveExprAndReplace(byItem.Expr, replace)
	}
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (lt *LogicalTopN) DeriveStats(childStats []*property.StatsInfo, selfSchema *expression.Schema, childSchema []*expression.Schema, _ [][]*expression.Column) (*property.StatsInfo, error) {
	if lt.stats != nil {
		return lt.stats, nil
	}
	lt.stats = deriveLimitStats(childStats[0], float64(lt.Count))
	return lt.stats, nil
}
