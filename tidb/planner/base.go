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

type baseImpl struct {
	cost float64
	plan PhysicalPlan
}

func (impl *baseImpl) CalcCost(outCount float64, children ...Implementation) float64 {
	impl.cost = 0
	for _, child := range children {
		impl.cost += child.GetCost()
	}
	return impl.cost
}

func (impl *baseImpl) SetCost(cost float64) {
	impl.cost = cost
}

func (impl *baseImpl) GetCost() float64 {
	return impl.cost
}

func (impl *baseImpl) GetPlan() PhysicalPlan {
	return impl.plan
}

func (impl *baseImpl) AttachChildren(children ...Implementation) Implementation {
	childrenPlan := make([]PhysicalPlan, len(children))
	for i, child := range children {
		childrenPlan[i] = child.GetPlan()
	}
	impl.plan.SetChildren(childrenPlan...)
	return impl
}

func (impl *baseImpl) ScaleCostLimit(costLimit float64) float64 {
	return costLimit
}

func (impl *baseImpl) GetCostLimit(costLimit float64, children ...Implementation) float64 {
	childrenCost := 0.0
	for _, child := range children {
		childrenCost += child.GetCost()
	}
	return costLimit - childrenCost
}
