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
	"github.com/squareup/pranadb/tidb/planner/util"
)

func getPossiblePropertyFromByItems(items []*util.ByItems) []*expression.Column {
	cols := make([]*expression.Column, 0, len(items))
	for _, item := range items {
		if col, ok := item.Expr.(*expression.Column); ok {
			cols = append(cols, col)
		} else {
			break
		}
	}
	return cols
}

// PreparePossibleProperties implements LogicalPlan PreparePossibleProperties interface.
func (p *baseLogicalPlan) PreparePossibleProperties(schema *expression.Schema, childrenProperties ...[][]*expression.Column) [][]*expression.Column {
	return nil
}

func clonePossibleProperties(props [][]*expression.Column) [][]*expression.Column {
	res := make([][]*expression.Column, len(props))
	for i, prop := range props {
		clonedProp := make([]*expression.Column, len(prop))
		for j, col := range prop {
			clonedProp[j] = col.Clone().(*expression.Column)
		}
		res[i] = clonedProp
	}
	return res
}
