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
	"encoding/binary"
	"github.com/squareup/pranadb/tidb/expression"
	"github.com/squareup/pranadb/tidb/planner/property"
	"github.com/squareup/pranadb/tidb/sessionctx"
)

// LogicalLimit represents offset and limit plan.
type LogicalLimit struct {
	logicalSchemaProducer

	Offset uint64
	Count  uint64
}

// Init initializes LogicalLimit.
func (p LogicalLimit) Init(ctx sessionctx.Context, offset int) *LogicalLimit {
	p.baseLogicalPlan = newBaseLogicalPlan(ctx, TypeLimit, &p, offset)
	return &p
}

// BuildKeyInfo implements LogicalPlan BuildKeyInfo interface.
func (p *LogicalLimit) BuildKeyInfo(selfSchema *expression.Schema, childSchema []*expression.Schema) {
	p.logicalSchemaProducer.BuildKeyInfo(selfSchema, childSchema)
	if p.Count == 1 {
		p.maxOneRow = true
	}
}

// HashCode implements LogicalPlan interface.
func (p *LogicalLimit) HashCode() []byte {
	// PlanType + SelectOffset + Offset + Count
	result := make([]byte, 24)
	binary.BigEndian.PutUint32(result, uint32(TypeStringToPhysicalID(p.tp)))
	binary.BigEndian.PutUint32(result[4:], uint32(p.SelectBlockOffset()))
	binary.BigEndian.PutUint64(result[8:], p.Offset)
	binary.BigEndian.PutUint64(result[16:], p.Count)
	return result
}

// PruneColumns implements LogicalPlan interface.
func (p *LogicalLimit) PruneColumns(parentUsedCols []*expression.Column) error {
	if len(parentUsedCols) == 0 { // happens when LIMIT appears in UPDATE.
		return nil
	}

	savedUsedCols := make([]*expression.Column, len(parentUsedCols))
	copy(savedUsedCols, parentUsedCols)
	if err := p.children[0].PruneColumns(parentUsedCols); err != nil {
		return err
	}
	p.schema = nil
	p.inlineProjection(savedUsedCols)
	return nil
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (p *LogicalLimit) DeriveStats(childStats []*property.StatsInfo, selfSchema *expression.Schema, childSchema []*expression.Schema, _ [][]*expression.Column) (*property.StatsInfo, error) {
	if p.stats != nil {
		return p.stats, nil
	}
	p.stats = deriveLimitStats(childStats[0], float64(p.Count))
	return p.stats, nil
}
