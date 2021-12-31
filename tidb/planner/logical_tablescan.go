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
	"github.com/pingcap/parser/mysql"
	"github.com/squareup/pranadb/tidb/expression"
	"github.com/squareup/pranadb/tidb/planner/property"
	"github.com/squareup/pranadb/tidb/sessionctx"
	"github.com/squareup/pranadb/tidb/util/ranger"
)

// LogicalTableScan is the logical table scan operator for TiKV.
type LogicalTableScan struct {
	logicalSchemaProducer
	Source      *LogicalDataSource
	HandleCols  HandleCols
	AccessConds expression.CNFExprs
	Ranges      []*ranger.Range
}

// Init initializes LogicalTableScan.
func (ts LogicalTableScan) Init(ctx sessionctx.Context, offset int) *LogicalTableScan {
	ts.baseLogicalPlan = newBaseLogicalPlan(ctx, TypeTableScan, &ts, offset)
	return &ts
}

// GetPhysicalScan returns PhysicalTableScan for the LogicalTableScan.
func (s *LogicalTableScan) GetPhysicalScan(schema *expression.Schema, stats *property.StatsInfo) *PhysicalTableScan {
	ds := s.Source
	ts := PhysicalTableScan{
		Table:           ds.tableInfo,
		Columns:         ds.Columns,
		TableAsName:     ds.TableAsName,
		DBName:          ds.DBName,
		Ranges:          s.Ranges,
		AccessCondition: s.AccessConds,
	}.Init(s.ctx, s.blockOffset)
	ts.stats = stats
	ts.SetSchema(schema.Clone())
	if ts.Table.PKIsHandle {
		if pkColInfo := ts.Table.GetPkColInfo(); pkColInfo != nil {
			if ds.statisticTable.Columns[pkColInfo.ID] != nil {
				ts.Hist = &ds.statisticTable.Columns[pkColInfo.ID].Histogram
			}
		}
	}
	return ts
}

// BuildKeyInfo implements LogicalPlan BuildKeyInfo interface.
func (ts *LogicalTableScan) BuildKeyInfo(selfSchema *expression.Schema, childSchema []*expression.Schema) {
	ts.Source.BuildKeyInfo(selfSchema, childSchema)
}

// PreparePossibleProperties implements LogicalPlan PreparePossibleProperties interface.
func (ts *LogicalTableScan) PreparePossibleProperties(schema *expression.Schema, childrenProperties ...[][]*expression.Column) [][]*expression.Column {
	if ts.HandleCols != nil {
		cols := make([]*expression.Column, ts.HandleCols.NumCols())
		for i := 0; i < ts.HandleCols.NumCols(); i++ {
			cols[i] = ts.HandleCols.GetCol(i)
		}
		return [][]*expression.Column{cols}
	}
	return nil
}

// DeriveStats implements LogicalPlan DeriveStats interface.
func (ts *LogicalTableScan) DeriveStats(childStats []*property.StatsInfo, selfSchema *expression.Schema, childSchema []*expression.Schema, _ [][]*expression.Column) (_ *property.StatsInfo, err error) {
	ts.Source.initStats(nil)
	// PushDownNot here can convert query 'not (a != 1)' to 'a = 1'.
	for i, expr := range ts.AccessConds {
		// TODO The expressions may be shared by TableScan and several IndexScans, there would be redundant
		// `PushDownNot` function call in multiple `DeriveStats` then.
		ts.AccessConds[i] = expression.PushDownNot(ts.ctx, expr)
	}
	ts.stats = ts.Source.deriveStatsByFilter(ts.AccessConds, nil)
	sc := ts.SCtx().GetSessionVars().StmtCtx
	// ts.Handle could be nil if PK is Handle, and PK column has been pruned.
	// TODO: support clustered index.
	if ts.HandleCols != nil {
		ts.Ranges, err = ranger.BuildTableRange(ts.AccessConds, sc, ts.HandleCols.GetCol(0).RetType)
	} else {
		isUnsigned := false
		if ts.Source.tableInfo.PKIsHandle {
			if pkColInfo := ts.Source.tableInfo.GetPkColInfo(); pkColInfo != nil {
				isUnsigned = mysql.HasUnsignedFlag(pkColInfo.Flag)
			}
		}
		ts.Ranges = ranger.FullIntRange(isUnsigned)
	}
	if err != nil {
		return nil, err
	}
	return ts.stats, nil
}
