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
	"github.com/squareup/pranadb/tidb/util/plancodec"
	"sort"
)

// LogicalProjection represents a select fields plan.
type LogicalProjection struct {
	logicalSchemaProducer

	Exprs []expression.Expression

	// CalculateNoDelay indicates this Projection is the root Plan and should be
	// calculated without delay and will not return any result to client.
	// Currently it is "true" only when the current sql query is a "DO" statement.
	// See "https://dev.mysql.com/doc/refman/5.7/en/do.html" for more detail.
	CalculateNoDelay bool

	// AvoidColumnEvaluator is a temporary variable which is ONLY used to avoid
	// building columnEvaluator for the expressions of Projection which is
	// built by buildProjection4Union.
	// This can be removed after column pool being supported.
	// Related issue: TiDB#8141(https://github.com/pingcap/tidb/issues/8141)
	AvoidColumnEvaluator bool
}

// Init initializes LogicalProjection.
func (p LogicalProjection) Init(ctx sessionctx.Context, offset int) *LogicalProjection {
	p.baseLogicalPlan = newBaseLogicalPlan(ctx, plancodec.TypeProj, &p, offset)
	return &p
}

// GetUsedCols extracts all of the Columns used by proj.
func (p *LogicalProjection) GetUsedCols() (usedCols []*expression.Column) {
	for _, expr := range p.Exprs {
		usedCols = append(usedCols, expression.ExtractColumns(expr)...)
	}
	return usedCols
}

// TryToGetChildProp will check if this sort property can be pushed or not.
// When a sort column will be replaced by scalar function, we refuse it.
// When a sort column will be replaced by a constant, we just remove it.
func (p *LogicalProjection) TryToGetChildProp(prop *property.PhysicalProperty) (*property.PhysicalProperty, bool) {
	newProp := prop.CloneEssentialFields()
	newCols := make([]property.SortItem, 0, len(prop.SortItems))
	for _, col := range prop.SortItems {
		idx := p.schema.ColumnIndex(col.Col)
		switch expr := p.Exprs[idx].(type) {
		case *expression.Column:
			newCols = append(newCols, property.SortItem{Col: expr, Desc: col.Desc})
		case *expression.ScalarFunction:
			return nil, false
		}
	}
	newProp.SortItems = newCols
	return newProp, true
}

// A bijection exists between columns of a projection's schema and this projection's Exprs.
// Sometimes we need a schema made by expr of Exprs to convert a column in child's schema to a column in this projection's Schema.
func (p *LogicalProjection) buildSchemaByExprs(selfSchema *expression.Schema) *expression.Schema {
	schema := expression.NewSchema(make([]*expression.Column, 0, selfSchema.Len())...)
	for _, expr := range p.Exprs {
		if col, isCol := expr.(*expression.Column); isCol {
			schema.Append(col)
		} else {
			// If the expression is not a column, we add a column to occupy the position.
			schema.Append(&expression.Column{
				UniqueID: p.ctx.GetSessionVars().AllocPlanColumnID(),
				RetType:  expr.GetType(),
			})
		}
	}
	return schema
}

// BuildKeyInfo implements LogicalPlan BuildKeyInfo interface.
func (p *LogicalProjection) BuildKeyInfo(selfSchema *expression.Schema, childSchema []*expression.Schema) {
	// `LogicalProjection` use schema from `Exprs` to build key info. See `buildSchemaByExprs`.
	// So call `baseLogicalPlan.BuildKeyInfo` here to avoid duplicated building key info.
	p.baseLogicalPlan.BuildKeyInfo(selfSchema, childSchema)
	selfSchema.Keys = nil
	schema := p.buildSchemaByExprs(selfSchema)
	for _, key := range childSchema[0].Keys {
		indices := schema.ColumnsIndices(key)
		if indices == nil {
			continue
		}
		newKey := make([]*expression.Column, 0, len(key))
		for _, i := range indices {
			newKey = append(newKey, selfSchema.Columns[i])
		}
		selfSchema.Keys = append(selfSchema.Keys, newKey)
	}
}

// PruneColumns implements LogicalPlan interface.
// If any expression has SetVar function or Sleep function, we do not prune it.
func (p *LogicalProjection) PruneColumns(parentUsedCols []*expression.Column) error {
	child := p.children[0]
	used := expression.GetUsedList(parentUsedCols, p.schema)

	for i := len(used) - 1; i >= 0; i-- {
		if !used[i] && !exprHasSetVarOrSleep(p.Exprs[i]) {
			p.schema.Columns = append(p.schema.Columns[:i], p.schema.Columns[i+1:]...)
			p.Exprs = append(p.Exprs[:i], p.Exprs[i+1:]...)
		}
	}
	selfUsedCols := make([]*expression.Column, 0, len(p.Exprs))
	selfUsedCols = expression.ExtractColumnsFromExpressions(selfUsedCols, p.Exprs, nil)
	return child.PruneColumns(selfUsedCols)
}

func (p *LogicalProjection) replaceExprColumns(replace map[string]*expression.Column) {
	for _, expr := range p.Exprs {
		resolveExprAndReplace(expr, replace)
	}
}

// PreparePossibleProperties implements LogicalPlan PreparePossibleProperties interface.
func (p *LogicalProjection) PreparePossibleProperties(schema *expression.Schema, childrenProperties ...[][]*expression.Column) [][]*expression.Column {
	childProperties := childrenProperties[0]
	oldCols := make([]*expression.Column, 0, p.schema.Len())
	newCols := make([]*expression.Column, 0, p.schema.Len())
	for i, expr := range p.Exprs {
		if col, ok := expr.(*expression.Column); ok {
			newCols = append(newCols, p.schema.Columns[i])
			oldCols = append(oldCols, col)
		}
	}
	tmpSchema := expression.NewSchema(oldCols...)
	newProperties := make([][]*expression.Column, 0, len(childProperties))
	for _, childProperty := range childProperties {
		newChildProperty := make([]*expression.Column, 0, len(childProperty))
		for _, col := range childProperty {
			pos := tmpSchema.ColumnIndex(col)
			if pos >= 0 {
				newChildProperty = append(newChildProperty, newCols[pos])
			} else {
				break
			}
		}
		if len(newChildProperty) != 0 {
			newProperties = append(newProperties, newChildProperty)
		}
	}
	return newProperties
}

func (p *LogicalProjection) getGroupNDVs(colGroups [][]*expression.Column, childProfile *property.StatsInfo, selfSchema *expression.Schema) []property.GroupNDV {
	if len(colGroups) == 0 || len(childProfile.GroupNDVs) == 0 {
		return nil
	}
	exprCol2ProjCol := make(map[int64]int64)
	for i, expr := range p.Exprs {
		exprCol, ok := expr.(*expression.Column)
		if !ok {
			continue
		}
		exprCol2ProjCol[exprCol.UniqueID] = selfSchema.Columns[i].UniqueID
	}
	ndvs := make([]property.GroupNDV, 0, len(childProfile.GroupNDVs))
	for _, childGroupNDV := range childProfile.GroupNDVs {
		projCols := make([]int64, len(childGroupNDV.Cols))
		for i, col := range childGroupNDV.Cols {
			projCol, ok := exprCol2ProjCol[col]
			if !ok {
				projCols = nil
				break
			}
			projCols[i] = projCol
		}
		if projCols == nil {
			continue
		}
		sort.Slice(projCols, func(i, j int) bool {
			return projCols[i] < projCols[j]
		})
		groupNDV := property.GroupNDV{
			Cols: projCols,
			NDV:  childGroupNDV.NDV,
		}
		ndvs = append(ndvs, groupNDV)
	}
	return ndvs
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (p *LogicalProjection) DeriveStats(childStats []*property.StatsInfo, selfSchema *expression.Schema, childSchema []*expression.Schema, colGroups [][]*expression.Column) (*property.StatsInfo, error) {
	childProfile := childStats[0]
	if p.stats != nil {
		// Reload GroupNDVs since colGroups may have changed.
		p.stats.GroupNDVs = p.getGroupNDVs(colGroups, childProfile, selfSchema)
		return p.stats, nil
	}
	p.stats = &property.StatsInfo{
		RowCount: childProfile.RowCount,
		ColNDVs:  make(map[int64]float64, len(p.Exprs)),
	}
	for i, expr := range p.Exprs {
		cols := expression.ExtractColumns(expr)
		p.stats.ColNDVs[selfSchema.Columns[i].UniqueID] = getColsNDV(cols, childSchema[0], childProfile)
	}
	p.stats.GroupNDVs = p.getGroupNDVs(colGroups, childProfile, selfSchema)
	return p.stats, nil
}

// ExtractColGroups implements LogicalPlan ExtractColGroups interface.
func (p *LogicalProjection) ExtractColGroups(colGroups [][]*expression.Column) [][]*expression.Column {
	if len(colGroups) == 0 {
		return nil
	}
	extColGroups, _ := p.Schema().ExtractColGroups(colGroups)
	if len(extColGroups) == 0 {
		return nil
	}
	extracted := make([][]*expression.Column, 0, len(extColGroups))
	for _, cols := range extColGroups {
		exprs := make([]*expression.Column, len(cols))
		allCols := true
		for i, offset := range cols {
			col, ok := p.Exprs[offset].(*expression.Column)
			// TODO: for functional dependent projections like `col1 + 1` -> `col2`, we can maintain GroupNDVs actually.
			if !ok {
				allCols = false
				break
			}
			exprs[i] = col
		}
		if allCols {
			extracted = append(extracted, expression.SortColumns(exprs))
		}
	}
	return extracted
}

// HashCode implements LogicalPlan interface.
func (p *LogicalProjection) HashCode() []byte {
	// PlanType + SelectOffset + ExprNum + [Exprs]
	// Expressions are commonly `Column`s, whose hashcode has the length 9, so
	// we pre-alloc 10 bytes for each expr's hashcode.
	result := make([]byte, 0, 12+len(p.Exprs)*10)
	result = encodeIntAsUint32(result, plancodec.TypeStringToPhysicalID(p.tp))
	result = encodeIntAsUint32(result, p.SelectBlockOffset())
	result = encodeIntAsUint32(result, len(p.Exprs))
	for _, expr := range p.Exprs {
		exprHashCode := expr.HashCode(p.ctx.GetSessionVars().StmtCtx)
		result = encodeIntAsUint32(result, len(exprHashCode))
		result = append(result, exprHashCode...)
	}
	return result
}
