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
	"github.com/pingcap/parser/mysql"
	"github.com/squareup/pranadb/tidb/expression"
	"github.com/squareup/pranadb/tidb/sessionctx"
	"github.com/squareup/pranadb/tidb/statistics"
	"github.com/squareup/pranadb/tidb/types"
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

	Covers bool
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
	indexCols := make([]*expression.Column, len(is.IdxCols), len(is.Index.Columns)+1)
	copy(indexCols, is.IdxCols)

	for i := len(is.IdxCols); i < len(is.Index.Columns); i++ {
		if idxExprCols[i] != nil {
			indexCols = append(indexCols, idxExprCols[i])
		} else {
			// TODO: try to reuse the col generated when building the LogicalDataSource.
			indexCols = append(indexCols, &expression.Column{
				ID:       is.Table.Columns[is.Index.Columns[i].Offset].ID,
				RetType:  &is.Table.Columns[is.Index.Columns[i].Offset].FieldType,
				UniqueID: is.ctx.GetSessionVars().AllocPlanColumnID(),
			})
		}
	}
	is.NeedCommonHandle = is.Table.IsCommonHandle
	is.Covers = !isDoubleRead

	if is.NeedCommonHandle {
		for i := len(is.Index.Columns); i < len(idxExprCols); i++ {
			indexCols = append(indexCols, idxExprCols[i])
		}
	}
	//setHandle := false
	// TODO - for some reason was adding the PK col again (resulting in same col twice) when the index was on the PK
	// Commenting out seems to fix - need to investigate more
	//setHandle := len(indexCols) > len(is.Index.Columns)
	//if !setHandle {
	//	for i, col := range is.Columns {
	//		if (mysql.HasPriKeyFlag(col.Flag) && is.Table.PKIsHandle) || col.ID == model.ExtraHandleID {
	//			indexCols = append(indexCols, is.dataSourceSchema.Columns[i])
	//			setHandle = true
	//			break
	//		}
	//	}
	//}

	if isDoubleRead {
		// If it's a double read, we need to add the queried columns that are not already in the index.
		for _, queryCol := range is.Columns {
			setQueryCol := true
			for _, idxCol := range is.IdxCols {
				if queryCol.ID == idxCol.ID {
					setQueryCol = false
				}
			}
			if setQueryCol {
				indexCols = append(indexCols, &expression.Column{
					RetType:  types.NewFieldType(queryCol.FieldType.Tp),
					ID:       queryCol.ID,
					UniqueID: queryCol.ID,
					OrigName: is.DBName.L + "." + is.Table.Name.L + "." + queryCol.Name.L,
				})
			}
		}
		// If it's double read case, the first index must return handle. So we should add extra handle column
		// if there isn't a handle column.
		//if !setHandle {
		//	if !is.Table.IsCommonHandle {
		//		indexCols = append(indexCols, &expression.Column{
		//			RetType:  types.NewFieldType(mysql.TypeLonglong),
		//			ID:       model.ExtraHandleID,
		//			UniqueID: is.ctx.GetSessionVars().AllocPlanColumnID(),
		//		})
		//	}
		//}
		// If index is global, we should add extra column for pid.
		if is.Index.Global {
			indexCols = append(indexCols, &expression.Column{
				RetType:  types.NewFieldType(mysql.TypeLonglong),
				ID:       model.ExtraPidColID,
				UniqueID: is.ctx.GetSessionVars().AllocPlanColumnID(),
			})
		}
	}

	is.SetSchema(expression.NewSchema(indexCols...))
}
