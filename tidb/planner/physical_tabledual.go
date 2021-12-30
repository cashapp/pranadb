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
	"github.com/squareup/pranadb/tidb/types"
	"github.com/squareup/pranadb/tidb/util/plancodec"
)

var _ PhysicalPlan = &PhysicalTableDual{}

// PhysicalTableDual is the physical operator of dual.
type PhysicalTableDual struct {
	physicalSchemaProducer

	RowCount int

	// names is used for OutputNames() method. Dual may be inited when building point get plan.
	// So it needs to hold names for itself.
	names []*types.FieldName
}

// Init initializes PhysicalTableDual.
func (p PhysicalTableDual) Init(ctx sessionctx.Context, stats *property.StatsInfo, offset int) *PhysicalTableDual {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, plancodec.TypeDual, &p, offset)
	p.stats = stats
	return &p
}

// OutputNames returns the outputting names of each column.
func (p *PhysicalTableDual) OutputNames() types.NameSlice {
	return p.names
}

// SetOutputNames sets the outputting name by the given slice.
func (p *PhysicalTableDual) SetOutputNames(names types.NameSlice) {
	p.names = names
}
