//
// This source code is a modified form of original source from the TiDB project, which has the following copyright header(s):
//

// Copyright 2015 PingCAP, Inc.
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
	"fmt"
	"github.com/squareup/pranadb/tidb/expression"
	"github.com/squareup/pranadb/tidb/planner/property"
	"github.com/squareup/pranadb/tidb/sessionctx"
	"github.com/squareup/pranadb/tidb/types"
	"strings"
)

// Plan is the description of an execution flow.
// It is created from ast.Node first, then optimized by the optimizer,
// finally used by the executor to create a Cursor which executes the statement.
type Plan interface {
	// Get the schema.
	Schema() *expression.Schema

	// Get the ID.
	ID() int

	SCtx() sessionctx.Context

	// property.StatsInfo will return the property.StatsInfo for this plan.
	statsInfo() *property.StatsInfo

	// OutputNames returns the outputting names of each column.
	OutputNames() types.NameSlice

	// SetOutputNames sets the outputting name by the given slice.
	SetOutputNames(names types.NameSlice)

	SelectBlockOffset() int
}

// LogicalPlan is a tree of logical operators.
// We can do a lot of logical optimizations to it, like predicate pushdown and column pruning.
type LogicalPlan interface {
	Plan

	// HashCode encodes a LogicalPlan to fast compare whether a LogicalPlan equals to another.
	// We use a strict encode method here which ensures there is no conflict.
	HashCode() []byte

	// PruneColumns prunes the unused columns.
	PruneColumns([]*expression.Column) error

	// BuildKeyInfo will collect the information of unique keys into schema.
	// Because this method is also used in cascades planner, we cannot use
	// things like `p.schema` or `p.children` inside it. We should use the `selfSchema`
	// and `childSchema` instead.
	BuildKeyInfo(selfSchema *expression.Schema, childSchema []*expression.Schema)

	// recursiveDeriveStats derives statistic info between plans.
	recursiveDeriveStats(colGroups [][]*expression.Column) (*property.StatsInfo, error)

	// DeriveStats derives statistic info for current plan node given child stats.
	// We need selfSchema, childSchema here because it makes this method can be used in
	// cascades planner, where LogicalPlan might not record its children or schema.
	DeriveStats(childStats []*property.StatsInfo, selfSchema *expression.Schema, childSchema []*expression.Schema, colGroups [][]*expression.Column) (*property.StatsInfo, error)

	// ExtractColGroups extracts column groups from child operator whose DNVs are required by the current operator.
	// For example, if current operator is LogicalAggregation of `Group By a, b`, we indicate the child operators to maintain
	// and propagate the NDV info of column group (a, b), to improve the row count estimation of current LogicalAggregation.
	// The parameter colGroups are column groups required by upper operators, besides from the column groups derived from
	// current operator, we should pass down parent colGroups to child operator as many as possible.
	ExtractColGroups(colGroups [][]*expression.Column) [][]*expression.Column

	// PreparePossibleProperties is only used for join and aggregation. Like group by a,b,c, all permutation of (a,b,c) is
	// valid, but the ordered indices in leaf plan is limited. So we can get all possible order properties by a pre-walking.
	PreparePossibleProperties(schema *expression.Schema, childrenProperties ...[][]*expression.Column) [][]*expression.Column

	// MaxOneRow means whether this operator only returns max one row.
	MaxOneRow() bool

	// Get all the children.
	Children() []LogicalPlan

	// SetChildren sets the children for the plan.
	SetChildren(...LogicalPlan)

	// SetChild sets the ith child for the plan.
	SetChild(i int, child LogicalPlan)

	String() string

	Dump() string
}

// PhysicalPlan is a tree of the physical operators.
type PhysicalPlan interface {
	Plan

	// getChildReqProps gets the required property by child index.
	GetChildReqProps(idx int) *property.PhysicalProperty

	// StatsCount returns the count of property.StatsInfo for this plan.
	StatsCount() float64

	// Get all the children.
	Children() []PhysicalPlan

	// SetChildren sets the children for the plan.
	SetChildren(...PhysicalPlan)

	// SetChild sets the ith child for the plan.
	SetChild(i int, child PhysicalPlan)

	// ResolveIndices resolves the indices for columns. After doing this, the columns can evaluate the rows by their indices.
	ResolveIndices() error

	// Stats returns the StatsInfo of the plan.
	Stats() *property.StatsInfo

	// Cost returns the estimated cost of the subplan.
	Cost() float64

	// SetCost set the cost of the subplan.
	SetCost(cost float64)
}

type baseLogicalPlan struct {
	basePlan

	self      LogicalPlan
	maxOneRow bool
	children  []LogicalPlan
}

func (p *baseLogicalPlan) MaxOneRow() bool {
	return p.maxOneRow
}

// PreparePossibleProperties implements LogicalPlan PreparePossibleProperties interface.
func (p *baseLogicalPlan) PreparePossibleProperties(schema *expression.Schema, childrenProperties ...[][]*expression.Column) [][]*expression.Column {
	return nil
}

func (p *baseLogicalPlan) Dump() string {
	builder := strings.Builder{}
	builder.WriteString(p.self.String())
	lc := len(p.children)
	if lc > 0 {
		builder.WriteString("|\n|\n|\nv\n")
	}
	for i, child := range p.children {
		if lc > 1 {
			builder.WriteString(fmt.Sprintf("============ Child %d\n", i))
		}
		builder.WriteString(child.Dump())
	}
	return builder.String()
}

type basePhysicalPlan struct {
	basePlan

	childrenReqProps []*property.PhysicalProperty
	self             PhysicalPlan
	children         []PhysicalPlan
	cost             float64
}

// Cost implements PhysicalPlan interface.
func (p *basePhysicalPlan) Cost() float64 {
	return p.cost
}

// SetCost implements PhysicalPlan interface.
func (p *basePhysicalPlan) SetCost(cost float64) {
	p.cost = cost
}

func (p *basePhysicalPlan) GetChildReqProps(idx int) *property.PhysicalProperty {
	return p.childrenReqProps[idx]
}

// ResolveIndices implements Plan interface.
func (p *basePhysicalPlan) ResolveIndices() (err error) {
	for _, child := range p.children {
		err = child.ResolveIndices()
		if err != nil {
			return err
		}
	}
	return
}

// HasMaxOneRow returns if the LogicalPlan will output at most one row.
func HasMaxOneRow(p LogicalPlan, childMaxOneRow []bool) bool {
	if len(childMaxOneRow) == 0 {
		// The reason why we use this check is that, this function
		// is used both in planner/core and planner/cascades.
		// In cascades planner, LogicalPlan may have no `children`.
		return false
	}
	switch x := p.(type) {
	case *LogicalLimit, *LogicalSort, *LogicalSelection,
		*LogicalProjection, *LogicalAggregation:
		return childMaxOneRow[0]
	case *LogicalJoin:
		switch x.JoinType {
		case SemiJoin, AntiSemiJoin, LeftOuterSemiJoin, AntiLeftOuterSemiJoin:
			return childMaxOneRow[0]
		default:
			return childMaxOneRow[0] && childMaxOneRow[1]
		}
	}
	return false
}

// BuildKeyInfo implements LogicalPlan BuildKeyInfo interface.
func (p *baseLogicalPlan) BuildKeyInfo(selfSchema *expression.Schema, childSchema []*expression.Schema) {
	childMaxOneRow := make([]bool, len(p.children))
	for i := range p.children {
		childMaxOneRow[i] = p.children[i].MaxOneRow()
	}
	p.maxOneRow = HasMaxOneRow(p.self, childMaxOneRow)
}

// BuildKeyInfo implements LogicalPlan BuildKeyInfo interface.
func (p *logicalSchemaProducer) BuildKeyInfo(selfSchema *expression.Schema, childSchema []*expression.Schema) {
	selfSchema.Keys = nil
	p.baseLogicalPlan.BuildKeyInfo(selfSchema, childSchema)

	// default implementation for plans has only one child: proprgate child keys
	// multi-children plans are likely to have particular implementation.
	if len(childSchema) == 1 {
		for _, key := range childSchema[0].Keys {
			indices := selfSchema.ColumnsIndices(key)
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
}

func newBasePlan(ctx sessionctx.Context, tp string, offset int) basePlan {
	ctx.GetSessionVars().PlanID++
	id := ctx.GetSessionVars().PlanID
	return basePlan{
		tp:          tp,
		id:          id,
		ctx:         ctx,
		blockOffset: offset,
	}
}

func newBaseLogicalPlan(ctx sessionctx.Context, tp string, self LogicalPlan, offset int) baseLogicalPlan {
	return baseLogicalPlan{
		basePlan: newBasePlan(ctx, tp, offset),
		self:     self,
	}
}

func newBasePhysicalPlan(ctx sessionctx.Context, tp string, self PhysicalPlan, offset int) basePhysicalPlan {
	return basePhysicalPlan{
		basePlan: newBasePlan(ctx, tp, offset),
		self:     self,
	}
}

// PruneColumns implements LogicalPlan interface.
func (p *baseLogicalPlan) PruneColumns(parentUsedCols []*expression.Column) error {
	if len(p.children) == 0 {
		return nil
	}
	return p.children[0].PruneColumns(parentUsedCols)
}

// basePlan implements base Plan interface.
// Should be used as embedded struct in Plan implementations.
type basePlan struct {
	tp          string
	id          int
	ctx         sessionctx.Context
	stats       *property.StatsInfo
	blockOffset int
}

// OutputNames returns the outputting names of each column.
func (p *basePlan) OutputNames() types.NameSlice {
	return nil
}

func (p *basePlan) SetOutputNames(names types.NameSlice) {
}

func (p *basePlan) replaceExprColumns(replace map[string]*expression.Column) {
}

// ID implements Plan ID interface.
func (p *basePlan) ID() int {
	return p.id
}

// property.StatsInfo implements the Plan interface.
func (p *basePlan) statsInfo() *property.StatsInfo {
	return p.stats
}
func (p *basePlan) SelectBlockOffset() int {
	return p.blockOffset
}

// Stats implements Plan Stats interface.
func (p *basePlan) Stats() *property.StatsInfo {
	return p.stats
}

// Schema implements Plan Schema interface.
func (p *baseLogicalPlan) Schema() *expression.Schema {
	return p.children[0].Schema()
}

func (p *baseLogicalPlan) OutputNames() types.NameSlice {
	return p.children[0].OutputNames()
}

func (p *baseLogicalPlan) SetOutputNames(names types.NameSlice) {
	p.children[0].SetOutputNames(names)
}

// Schema implements Plan Schema interface.
func (p *basePhysicalPlan) Schema() *expression.Schema {
	return p.children[0].Schema()
}

// Children implements LogicalPlan Children interface.
func (p *baseLogicalPlan) Children() []LogicalPlan {
	return p.children
}

// Children implements PhysicalPlan Children interface.
func (p *basePhysicalPlan) Children() []PhysicalPlan {
	return p.children
}

// SetChildren implements LogicalPlan SetChildren interface.
func (p *baseLogicalPlan) SetChildren(children ...LogicalPlan) {
	p.children = children
}

// HashCode implements LogicalPlan interface.
func (p *baseLogicalPlan) HashCode() []byte {
	// We use PlanID for the default hash, so if two plans do not have
	// the same id, the hash value will never be the same.
	result := make([]byte, 0, 4)
	result = encodeIntAsUint32(result, p.id)
	return result
}

// SetChildren implements PhysicalPlan SetChildren interface.
func (p *basePhysicalPlan) SetChildren(children ...PhysicalPlan) {
	p.children = children
}

// SetChild implements LogicalPlan SetChild interface.
func (p *baseLogicalPlan) SetChild(i int, child LogicalPlan) {
	p.children[i] = child
}

// SetChild implements PhysicalPlan SetChild interface.
func (p *basePhysicalPlan) SetChild(i int, child PhysicalPlan) {
	p.children[i] = child
}

// Context implements Plan Context interface.
func (p *basePlan) SCtx() sessionctx.Context {
	return p.ctx
}
