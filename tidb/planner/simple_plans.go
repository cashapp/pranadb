//
// This source code is a modified form of original source from the TiDB project, which has the following copyright header(s):
//

// Copyright 2019 PingCAP, Inc.
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

// ProjectionImpl is the implementation of PhysicalProjection.
type ProjectionImpl struct {
	baseImpl
}

// NewProjectionImpl creates a new projection Implementation.
func NewProjectionImpl(proj *PhysicalProjection) *ProjectionImpl {
	return &ProjectionImpl{baseImpl{plan: proj}}
}

// CalcCost implements Implementation CalcCost interface.
func (impl *ProjectionImpl) CalcCost(outCount float64, children ...Implementation) float64 {
	proj := impl.plan.(*PhysicalProjection)
	impl.cost = proj.GetCost(children[0].GetPlan().Stats().RowCount) + children[0].GetCost()
	return impl.cost
}

// TiDBSelectionImpl is the implementation of PhysicalSelection in TiDB layer.
type TiDBSelectionImpl struct {
	baseImpl
}

// CalcCost implements Implementation CalcCost interface.
func (sel *TiDBSelectionImpl) CalcCost(outCount float64, children ...Implementation) float64 {
	sel.cost = children[0].GetPlan().Stats().RowCount*sel.plan.SCtx().GetSessionVars().CPUFactor + children[0].GetCost()
	return sel.cost
}

// NewTiDBSelectionImpl creates a new TiDBSelectionImpl.
func NewTiDBSelectionImpl(sel *PhysicalSelection) *TiDBSelectionImpl {
	return &TiDBSelectionImpl{baseImpl{plan: sel}}
}

// TiDBHashAggImpl is the implementation of PhysicalHashAgg in TiDB layer.
type TiDBHashAggImpl struct {
	baseImpl
}

// CalcCost implements Implementation CalcCost interface.
func (agg *TiDBHashAggImpl) CalcCost(outCount float64, children ...Implementation) float64 {
	hashAgg := agg.plan.(*PhysicalHashAgg)
	selfCost := hashAgg.GetCost(children[0].GetPlan().Stats().RowCount, true, false)
	agg.cost = selfCost + children[0].GetCost()
	return agg.cost
}

// AttachChildren implements Implementation AttachChildren interface.
func (agg *TiDBHashAggImpl) AttachChildren(children ...Implementation) Implementation {
	hashAgg := agg.plan.(*PhysicalHashAgg)
	hashAgg.SetChildren(children[0].GetPlan())
	return agg
}

// NewTiDBHashAggImpl creates a new TiDBHashAggImpl.
func NewTiDBHashAggImpl(agg *PhysicalHashAgg) *TiDBHashAggImpl {
	return &TiDBHashAggImpl{baseImpl{plan: agg}}
}

// LimitImpl is the implementation of PhysicalLimit. Since PhysicalLimit on different
// engines have the same behavior, and we don't calculate the cost of `Limit`, we only
// have one Implementation for it.
type LimitImpl struct {
	baseImpl
}

// NewLimitImpl creates a new LimitImpl.
func NewLimitImpl(limit *PhysicalLimit) *LimitImpl {
	return &LimitImpl{baseImpl{plan: limit}}
}

// TiDBTopNImpl is the implementation of PhysicalTopN in TiDB layer.
type TiDBTopNImpl struct {
	baseImpl
}

// CalcCost implements Implementation CalcCost interface.
func (impl *TiDBTopNImpl) CalcCost(outCount float64, children ...Implementation) float64 {
	topN := impl.plan.(*PhysicalTopN)
	childCount := children[0].GetPlan().Stats().RowCount
	impl.cost = topN.GetCost(childCount, true) + children[0].GetCost()
	return impl.cost
}

// NewTiDBTopNImpl creates a new TiDBTopNImpl.
func NewTiDBTopNImpl(topN *PhysicalTopN) *TiDBTopNImpl {
	return &TiDBTopNImpl{baseImpl{plan: topN}}
}

// UnionAllImpl is the implementation of PhysicalUnionAll.
type UnionAllImpl struct {
	baseImpl
}

// CalcCost implements Implementation CalcCost interface.
func (impl *UnionAllImpl) CalcCost(outCount float64, children ...Implementation) float64 {
	var childMaxCost float64
	for _, child := range children {
		childCost := child.GetCost()
		if childCost > childMaxCost {
			childMaxCost = childCost
		}
	}
	selfCost := float64(1+len(children)) * impl.plan.SCtx().GetSessionVars().ConcurrencyFactor
	// Children of UnionAll are executed in parallel.
	impl.cost = selfCost + childMaxCost
	return impl.cost
}

// GetCostLimit implements Implementation interface.
func (impl *UnionAllImpl) GetCostLimit(costLimit float64, children ...Implementation) float64 {
	return costLimit
}

// NewUnionAllImpl creates a new UnionAllImpl.
func NewUnionAllImpl(union *PhysicalUnionAll) *UnionAllImpl {
	return &UnionAllImpl{baseImpl{plan: union}}
}
