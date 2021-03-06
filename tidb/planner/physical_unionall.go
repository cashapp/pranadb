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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package planner

import (
	"github.com/squareup/pranadb/tidb/planner/property"
	"github.com/squareup/pranadb/tidb/sessionctx"
)

var _ PhysicalPlan = &PhysicalUnionAll{}

// PhysicalUnionAll is the physical operator of UnionAll.
type PhysicalUnionAll struct {
	physicalSchemaProducer
}

// Init initializes PhysicalUnionAll.
func (p PhysicalUnionAll) Init(ctx sessionctx.Context, stats *property.StatsInfo, offset int, props ...*property.PhysicalProperty) *PhysicalUnionAll {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, TypeUnion, &p, offset)
	p.childrenReqProps = props
	p.stats = stats
	return &p
}
