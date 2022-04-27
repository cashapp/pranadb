//
// This source code is a modified form of original source from the TiDB project, which has the following copyright header(s):
//

// Copyright 2018 PingCAP, Inc.
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
	"math"
)

// SortImpl implementation of PhysicalSort.
type SortImpl struct {
	baseImpl
}

// NewSortImpl creates a new sort Implementation.
func NewSortImpl(sort *PhysicalSort) *SortImpl {
	return &SortImpl{baseImpl{plan: sort}}
}

// CalcCost calculates the cost of the sort Implementation.
func (impl *SortImpl) CalcCost(outCount float64, children ...Implementation) float64 {
	cnt := math.Min(children[0].GetPlan().Stats().RowCount, impl.plan.GetChildReqProps(0).ExpectedCnt)
	sort := impl.plan.(*PhysicalSort)
	impl.cost = sort.GetCost(cnt, children[0].GetPlan().Schema()) + children[0].GetCost()
	return impl.cost
}

// AttachChildren implements Implementation AttachChildren interface.
func (impl *SortImpl) AttachChildren(children ...Implementation) Implementation {
	sort := impl.plan.(*PhysicalSort)
	sort.SetChildren(children[0].GetPlan())
	// When the Sort orderByItems contain ScalarFunction, we need
	// to inject two Projections below and above the Sort.
	impl.plan = injectProjBelowSort(sort, sort.ByItems)
	return impl
}

// NominalSortImpl is the implementation of NominalSort.
type NominalSortImpl struct {
	baseImpl
}

// AttachChildren implements Implementation AttachChildren interface.
func (impl *NominalSortImpl) AttachChildren(children ...Implementation) Implementation {
	return children[0]
}

// NewNominalSortImpl creates a new NominalSort Implementation.
func NewNominalSortImpl(sort *NominalSort) *NominalSortImpl {
	return &NominalSortImpl{baseImpl{plan: sort}}
}

// injectProjBelowSort extracts the ScalarFunctions of `orderByItems` into a
// PhysicalProjection and injects it below PhysicalTopN/PhysicalSort. The schema
// of PhysicalSort and PhysicalTopN are the same as the schema of their
// children. When a projection is injected as the child of PhysicalSort and
// PhysicalTopN, some extra columns will be added into the schema of the
// Projection, thus we need to add another Projection upon them to prune the
// redundant columns.
func injectProjBelowSort(p PhysicalPlan, orderByItems []*util.ByItems) PhysicalPlan {
	hasScalarFunc, numOrderByItems := false, len(orderByItems)
	for i := 0; !hasScalarFunc && i < numOrderByItems; i++ {
		_, isScalarFunc := orderByItems[i].Expr.(*expression.ScalarFunction)
		hasScalarFunc = hasScalarFunc || isScalarFunc
	}
	if !hasScalarFunc {
		return p
	}

	topProjExprs := make([]expression.Expression, 0, p.Schema().Len())
	for i := range p.Schema().Columns {
		col := p.Schema().Columns[i].Clone().(*expression.Column)
		col.Index = i
		topProjExprs = append(topProjExprs, col)
	}
	topProj := PhysicalProjection{
		Exprs:                topProjExprs,
		AvoidColumnEvaluator: false,
	}.Init(p.SCtx(), p.statsInfo(), p.SelectBlockOffset(), nil)
	topProj.SetSchema(p.Schema().Clone())
	topProj.SetChildren(p)

	childPlan := p.Children()[0]
	bottomProjSchemaCols := make([]*expression.Column, 0, len(childPlan.Schema().Columns)+numOrderByItems)
	bottomProjExprs := make([]expression.Expression, 0, len(childPlan.Schema().Columns)+numOrderByItems)
	for _, col := range childPlan.Schema().Columns {
		newCol := col.Clone().(*expression.Column)
		newCol.Index = childPlan.Schema().ColumnIndex(newCol)
		bottomProjSchemaCols = append(bottomProjSchemaCols, newCol)
		bottomProjExprs = append(bottomProjExprs, newCol)
	}

	for _, item := range orderByItems {
		itemExpr := item.Expr
		if _, isScalarFunc := itemExpr.(*expression.ScalarFunction); !isScalarFunc {
			continue
		}
		bottomProjExprs = append(bottomProjExprs, itemExpr)
		newArg := &expression.Column{
			UniqueID: p.SCtx().GetSessionVars().AllocPlanColumnID(),
			RetType:  itemExpr.GetType(),
			Index:    len(bottomProjSchemaCols),
		}
		bottomProjSchemaCols = append(bottomProjSchemaCols, newArg)
		item.Expr = newArg
	}

	childProp := p.GetChildReqProps(0).CloneEssentialFields()
	bottomProj := PhysicalProjection{
		Exprs:                bottomProjExprs,
		AvoidColumnEvaluator: false,
	}.Init(p.SCtx(), childPlan.statsInfo().ScaleByExpectCnt(childProp.ExpectedCnt), p.SelectBlockOffset(), childProp)
	bottomProj.SetSchema(expression.NewSchema(bottomProjSchemaCols...))
	bottomProj.SetChildren(childPlan)
	p.SetChildren(bottomProj)

	if origChildProj, isChildProj := childPlan.(*PhysicalProjection); isChildProj {
		refine4NeighbourProj(bottomProj, origChildProj)
	}

	return topProj
}
