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
	"github.com/pingcap/parser/model"
	"github.com/squareup/pranadb/tidb/expression"
	"github.com/squareup/pranadb/tidb/sessionctx"
	"github.com/squareup/pranadb/tidb/statistics"
	"github.com/squareup/pranadb/tidb/util/ranger"
)

var _ PhysicalPlan = &PhysicalIndexScan{}

// PhysicalIndexScan represents an index scan plan.
type PhysicalIndexScan struct {
	physicalSchemaProducer

	Table      *model.TableInfo
	Index      *model.IndexInfo
	IdxCols    []*expression.Column
	IdxColLens []int
	Ranges     []*ranger.Range
	Columns    []*model.ColumnInfo
	DBName     model.CIStr

	TableAsName *model.CIStr

	// dataSourceSchema is the original schema of LogicalDataSource. The schema of index scan in KV and index reader in TiDB
	// will be different. The schema of index scan will decode all columns of index but the TiDB only need some of them.
	dataSourceSchema *expression.Schema

	// Hist is the histogram when the query was issued.
	// It is used for query feedback.
	Hist *statistics.Histogram

	GenExprs map[model.TableColumnID]expression.Expression

	Desc      bool
	KeepOrder bool

	NeedCommonHandle bool
}

// Init initializes PhysicalIndexScan.
func (p PhysicalIndexScan) Init(ctx sessionctx.Context, offset int) *PhysicalIndexScan {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, TypeIdxScan, &p, offset)
	return &p
}

// initSchema is used to set the schema of PhysicalIndexScan. Before calling this,
// make sure the following field of PhysicalIndexScan are initialized:
//   PhysicalIndexScan.Table         *model.TableInfo
//   PhysicalIndexScan.Index         *model.IndexInfo
//   PhysicalIndexScan.Index.Columns []*IndexColumn
//   PhysicalIndexScan.IdxCols       []*expression.Column
//   PhysicalIndexScan.Columns       []*model.ColumnInfo
func (is *PhysicalIndexScan) initSchema(idxExprCols []*expression.Column, isDoubleRead bool) {
	// Should always be the same as logical schema
	is.SetSchema(is.dataSourceSchema)
}
