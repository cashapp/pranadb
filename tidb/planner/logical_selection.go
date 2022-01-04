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
	"bytes"
	"fmt"
	"github.com/pingcap/parser/ast"
	"github.com/squareup/pranadb/tidb/expression"
	"github.com/squareup/pranadb/tidb/planner/property"
	"github.com/squareup/pranadb/tidb/sessionctx"
	"sort"
	"strings"
)

// LogicalSelection represents a where or having predicate.
type LogicalSelection struct {
	baseLogicalPlan

	// Originally the WHERE or ON condition is parsed into a single expression,
	// but after we converted to CNF(Conjunctive normal form), it can be
	// split into a list of AND conditions.
	Conditions []expression.Expression

	// having selection can't be pushed down, because it must above the aggregation.
	buildByHaving bool
}

// Init initializes LogicalSelection.
func (p LogicalSelection) Init(ctx sessionctx.Context, offset int) *LogicalSelection {
	p.baseLogicalPlan = newBaseLogicalPlan(ctx, TypeSel, &p, offset)
	return &p
}

// If a condition is the form of (uniqueKey = constant) or (uniqueKey = Correlated column), it returns at most one row.
// This function will check it.
func (p *LogicalSelection) checkMaxOneRowCond(eqColIDs map[int64]struct{}, childSchema *expression.Schema) bool {
	if len(eqColIDs) == 0 {
		return false
	}
	// We check `UniqueKeys` as well since the condition is `col = con | corr`, not `col <=> con | corr`.
	keys := make([]expression.KeyInfo, 0, len(childSchema.Keys)+len(childSchema.UniqueKeys))
	keys = append(keys, childSchema.Keys...)
	keys = append(keys, childSchema.UniqueKeys...)
	var maxOneRow bool
	for _, cols := range keys {
		maxOneRow = true
		for _, c := range cols {
			if _, ok := eqColIDs[c.UniqueID]; !ok {
				maxOneRow = false
				break
			}
		}
		if maxOneRow {
			return true
		}
	}
	return false
}

// BuildKeyInfo implements LogicalPlan BuildKeyInfo interface.
func (p *LogicalSelection) BuildKeyInfo(selfSchema *expression.Schema, childSchema []*expression.Schema) {
	p.baseLogicalPlan.BuildKeyInfo(selfSchema, childSchema)
	if p.maxOneRow {
		return
	}
	eqCols := make(map[int64]struct{}, len(childSchema[0].Columns))
	for _, cond := range p.Conditions {
		if sf, ok := cond.(*expression.ScalarFunction); ok && sf.FuncName.L == ast.EQ {
			for i, arg := range sf.GetArgs() {
				if col, isCol := arg.(*expression.Column); isCol {
					_, isCon := sf.GetArgs()[1-i].(*expression.Constant)
					_, isCorCol := sf.GetArgs()[1-i].(*expression.CorrelatedColumn)
					if isCon || isCorCol {
						eqCols[col.UniqueID] = struct{}{}
					}
					break
				}
			}
		}
	}
	p.maxOneRow = p.checkMaxOneRowCond(eqCols, childSchema[0])
}

// HashCode implements LogicalPlan interface.
func (p *LogicalSelection) HashCode() []byte {
	// PlanType + SelectOffset + ConditionNum + [Conditions]
	// Conditions are commonly `ScalarFunction`s, whose hashcode usually has a
	// length larger than 20, so we pre-alloc 25 bytes for each expr's hashcode.
	result := make([]byte, 0, 12+len(p.Conditions)*25)
	result = encodeIntAsUint32(result, TypeStringToPhysicalID(p.tp))
	result = encodeIntAsUint32(result, p.SelectBlockOffset())
	result = encodeIntAsUint32(result, len(p.Conditions))

	condHashCodes := make([][]byte, len(p.Conditions))
	for i, expr := range p.Conditions {
		condHashCodes[i] = expr.HashCode(p.ctx.GetSessionVars().StmtCtx)
	}
	// Sort the conditions, so `a > 1 and a < 100` can equal to `a < 100 and a > 1`.
	sort.Slice(condHashCodes, func(i, j int) bool { return bytes.Compare(condHashCodes[i], condHashCodes[j]) < 0 })

	for _, condHashCode := range condHashCodes {
		result = encodeIntAsUint32(result, len(condHashCode))
		result = append(result, condHashCode...)
	}
	return result
}

// PruneColumns implements LogicalPlan interface.
func (p *LogicalSelection) PruneColumns(parentUsedCols []*expression.Column) error {
	child := p.children[0]
	parentUsedCols = expression.ExtractColumnsFromExpressions(parentUsedCols, p.Conditions, nil)
	return child.PruneColumns(parentUsedCols)
}

// PreparePossibleProperties implements LogicalPlan PreparePossibleProperties interface.
func (p *LogicalSelection) PreparePossibleProperties(schema *expression.Schema, childrenProperties ...[][]*expression.Column) [][]*expression.Column {
	return childrenProperties[0]
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (p *LogicalSelection) DeriveStats(childStats []*property.StatsInfo, selfSchema *expression.Schema, childSchema []*expression.Schema, _ [][]*expression.Column) (*property.StatsInfo, error) {
	if p.stats != nil {
		return p.stats, nil
	}
	p.stats = childStats[0].Scale(SelectionFactor)
	p.stats.GroupNDVs = nil
	return p.stats, nil
}

func (p *LogicalSelection) replaceExprColumns(replace map[string]*expression.Column) {
	for _, expr := range p.Conditions {
		resolveExprAndReplace(expr, replace)
	}
}

func (p *LogicalSelection) String() string {
	builder := strings.Builder{}
	builder.WriteString("Selection:\n")
	for _, cond := range p.Conditions {
		builder.WriteString(fmt.Sprintf("Condition:%s\n", cond))
	}
	return builder.String()
}
