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
	"github.com/squareup/pranadb/tidb/sessionctx"
	"github.com/squareup/pranadb/tidb/types"
	"strings"
)

// LogicalUnionAll represents LogicalUnionAll plan.
type LogicalUnionAll struct {
	logicalSchemaProducer
}

// Init initializes LogicalUnionAll.
func (p LogicalUnionAll) Init(ctx sessionctx.Context, offset int) *LogicalUnionAll {
	p.baseLogicalPlan = newBaseLogicalPlan(ctx, TypeUnion, &p, offset)
	return &p
}

func (b *PlanBuilder) buildProjection4Union(u *LogicalUnionAll) {
	unionCols := make([]*expression.Column, 0, u.children[0].Schema().Len())
	names := make([]*types.FieldName, 0, u.children[0].Schema().Len())

	// Infer union result types by its children's schema.
	for i, col := range u.children[0].Schema().Columns {
		tmpExprs := make([]expression.Expression, 0, len(u.Children()))
		tmpExprs = append(tmpExprs, col)
		resultTp := col.RetType
		for j := 1; j < len(u.children); j++ {
			tmpExprs = append(tmpExprs, u.children[j].Schema().Columns[i])
			childTp := u.children[j].Schema().Columns[i].RetType
			resultTp = unionJoinFieldType(resultTp, childTp)
		}
		resultTp.Charset, resultTp.Collate = expression.DeriveCollationFromExprs(b.ctx, tmpExprs...)
		names = append(names, &types.FieldName{ColName: u.children[0].OutputNames()[i].ColName})
		unionCols = append(unionCols, &expression.Column{
			RetType:  resultTp,
			UniqueID: b.ctx.GetSessionVars().AllocPlanColumnID(),
		})
	}
	u.schema = expression.NewSchema(unionCols...)
	u.names = names
	// Process each child and add a projection above original child.
	// So the schema of `UnionAll` can be the same with its children's.
	for childID, child := range u.children {
		exprs := make([]expression.Expression, len(child.Schema().Columns))
		for i, srcCol := range child.Schema().Columns {
			dstType := unionCols[i].RetType
			srcType := srcCol.RetType
			if !srcType.Equal(dstType) {
				exprs[i] = expression.BuildCastFunction4Union(b.ctx, srcCol, dstType)
			} else {
				exprs[i] = srcCol
			}
		}
		b.optFlag |= flagEliminateProjection
		proj := LogicalProjection{Exprs: exprs, AvoidColumnEvaluator: true}.Init(b.ctx, b.getSelectOffset())
		proj.SetSchema(u.schema.Clone())
		// reset the schema type to make the "not null" flag right.
		for i, expr := range exprs {
			proj.schema.Columns[i].RetType = expr.GetType()
		}
		proj.SetChildren(child)
		u.children[childID] = proj
	}
}

// PruneColumns implements LogicalPlan interface.
func (p *LogicalUnionAll) PruneColumns(parentUsedCols []*expression.Column) error {
	used := expression.GetUsedList(parentUsedCols, p.schema)
	hasBeenUsed := false
	for i := range used {
		hasBeenUsed = hasBeenUsed || used[i]
		if hasBeenUsed {
			break
		}
	}
	if !hasBeenUsed {
		parentUsedCols = make([]*expression.Column, len(p.schema.Columns))
		copy(parentUsedCols, p.schema.Columns)
	}
	for _, child := range p.Children() {
		err := child.PruneColumns(parentUsedCols)
		if err != nil {
			return err
		}
	}

	if hasBeenUsed {
		// keep the schema of LogicalUnionAll same as its children's
		used := expression.GetUsedList(p.children[0].Schema().Columns, p.schema)
		for i := len(used) - 1; i >= 0; i-- {
			if !used[i] {
				p.schema.Columns = append(p.schema.Columns[:i], p.schema.Columns[i+1:]...)
			}
		}
	}
	return nil
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (p *LogicalUnionAll) DeriveStats(childStats []*property.StatsInfo, selfSchema *expression.Schema, childSchema []*expression.Schema, _ [][]*expression.Column) (*property.StatsInfo, error) {
	if p.stats != nil {
		return p.stats, nil
	}
	p.stats = &property.StatsInfo{
		ColNDVs: make(map[int64]float64, selfSchema.Len()),
	}
	for _, childProfile := range childStats {
		p.stats.RowCount += childProfile.RowCount
		for _, col := range selfSchema.Columns {
			p.stats.ColNDVs[col.UniqueID] += childProfile.ColNDVs[col.UniqueID]
		}
	}
	return p.stats, nil
}

func (p *LogicalUnionAll) String() string {
	builder := strings.Builder{}
	builder.WriteString("UnionAll\n")
	builder.WriteString("Schema: ")
	builder.WriteString(p.schema.String())
	return builder.String()
}
