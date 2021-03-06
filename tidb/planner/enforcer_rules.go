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
	"math"

	"github.com/squareup/pranadb/tidb/planner/property"
	"github.com/squareup/pranadb/tidb/planner/util"
)

// Enforcer defines the interface for enforcer rules.
type Enforcer interface {
	// NewProperty generates relaxed property with the help of enforcer.
	NewProperty(prop *property.PhysicalProperty) (newProp *property.PhysicalProperty)
	// OnEnforce adds physical operators on top of child implementation to satisfy
	// required physical property.
	OnEnforce(reqProp *property.PhysicalProperty, child Implementation) (impl Implementation)
	// GetEnforceCost calculates cost of enforcing required physical property.
	GetEnforceCost(g *Group) float64
}

// GetEnforcerRules gets all candidate enforcer rules based
// on required physical property.
func GetEnforcerRules(g *Group, prop *property.PhysicalProperty) (enforcers []Enforcer) {
	if !prop.IsEmpty() {
		enforcers = append(enforcers, orderEnforcer)
	}
	return
}

// OrderEnforcer enforces order property on child implementation.
type OrderEnforcer struct {
}

var orderEnforcer = &OrderEnforcer{}

// NewProperty removes order property from required physical property.
func (e *OrderEnforcer) NewProperty(prop *property.PhysicalProperty) (newProp *property.PhysicalProperty) {
	// Order property cannot be empty now.
	newProp = &property.PhysicalProperty{ExpectedCnt: math.MaxFloat64}
	return
}

// OnEnforce adds sort operator to satisfy required order property.
func (e *OrderEnforcer) OnEnforce(reqProp *property.PhysicalProperty, child Implementation) (impl Implementation) {
	childPlan := child.GetPlan()
	sort := PhysicalSort{
		ByItems: make([]*util.ByItems, 0, len(reqProp.SortItems)),
	}.Init(childPlan.SCtx(), childPlan.Stats(), childPlan.SelectBlockOffset(), &property.PhysicalProperty{ExpectedCnt: math.MaxFloat64})
	for _, item := range reqProp.SortItems {
		item := &util.ByItems{
			Expr: item.Col,
			Desc: item.Desc,
		}
		sort.ByItems = append(sort.ByItems, item)
	}
	impl = NewSortImpl(sort).AttachChildren(child)
	return
}

// GetEnforceCost calculates cost of sort operator.
func (e *OrderEnforcer) GetEnforceCost(g *Group) float64 {
	// We need a SessionCtx to calculate the cost of a sort.
	sctx := g.Equivalents.Front().Value.(*GroupExpr).ExprNode.SCtx()
	sort := PhysicalSort{}.Init(sctx, g.Prop.Stats, 0, nil)
	cost := sort.GetCost(g.Prop.Stats.RowCount, g.Prop.Schema)
	return cost
}
