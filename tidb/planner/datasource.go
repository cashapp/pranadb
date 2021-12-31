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

import (
	"github.com/squareup/pranadb/tidb/expression"
	"github.com/squareup/pranadb/tidb/statistics"
)

// TableScanImpl implementation of PhysicalTableScan.
type TableScanImpl struct {
	baseImpl
	tblColHists *statistics.HistColl
	tblCols     []*expression.Column
}

// NewTableScanImpl creates a new table scan Implementation.
func NewTableScanImpl(ts *PhysicalTableScan, cols []*expression.Column, hists *statistics.HistColl) *TableScanImpl {
	base := baseImpl{plan: ts}
	impl := &TableScanImpl{
		baseImpl:    base,
		tblColHists: hists,
		tblCols:     cols,
	}
	return impl
}

// CalcCost calculates the cost of the table scan Implementation.
func (impl *TableScanImpl) CalcCost(outCount float64, children ...Implementation) float64 {
	ts := impl.plan.(*PhysicalTableScan)
	width := impl.tblColHists.GetTableAvgRowSize(impl.plan.SCtx(), impl.tblCols, true)
	sessVars := ts.SCtx().GetSessionVars()
	impl.cost = outCount * sessVars.GetScanFactor(ts.Table) * width
	if ts.Desc {
		impl.cost = outCount * sessVars.GetDescScanFactor(ts.Table) * width
	}
	return impl.cost
}

// IndexScanImpl is the Implementation of PhysicalIndexScan.
type IndexScanImpl struct {
	baseImpl
	tblColHists *statistics.HistColl
}

// CalcCost implements Implementation interface.
func (impl *IndexScanImpl) CalcCost(outCount float64, children ...Implementation) float64 {
	is := impl.plan.(*PhysicalIndexScan)
	sessVars := is.SCtx().GetSessionVars()
	rowSize := impl.tblColHists.GetIndexAvgRowSize(is.SCtx(), is.Schema().Columns, is.Index.Unique)
	cost := outCount * rowSize * sessVars.GetScanFactor(is.Table)
	if is.Desc {
		cost = outCount * rowSize * sessVars.GetDescScanFactor(is.Table)
	}
	cost += float64(len(is.Ranges)) * sessVars.GetSeekFactor(is.Table)
	impl.cost = cost
	return impl.cost
}

// NewIndexScanImpl creates a new IndexScan Implementation.
func NewIndexScanImpl(scan *PhysicalIndexScan, tblColHists *statistics.HistColl) *IndexScanImpl {
	return &IndexScanImpl{
		baseImpl:    baseImpl{plan: scan},
		tblColHists: tblColHists,
	}
}
