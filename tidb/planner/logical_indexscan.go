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
	"fmt"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/squareup/pranadb/tidb/expression"
	"github.com/squareup/pranadb/tidb/planner/property"
	"github.com/squareup/pranadb/tidb/sessionctx"
	"github.com/squareup/pranadb/tidb/types"
	"github.com/squareup/pranadb/tidb/util/ranger"
	"strings"
)

// LogicalIndexScan is the logical index scan operator for TiKV.
type LogicalIndexScan struct {
	logicalSchemaProducer
	// LogicalDataSource should be read-only here.
	Source       *LogicalDataSource
	IsDoubleRead bool

	EqCondCount int
	AccessConds expression.CNFExprs
	Ranges      []*ranger.Range

	Index          *model.IndexInfo
	Columns        []*model.ColumnInfo
	FullIdxCols    []*expression.Column
	FullIdxColLens []int
	IdxCols        []*expression.Column
	IdxColLens     []int
}

// Init initializes LogicalIndexScan.
func (is LogicalIndexScan) Init(ctx sessionctx.Context, offset int) *LogicalIndexScan {
	is.baseLogicalPlan = newBaseLogicalPlan(ctx, TypeIdxScan, &is, offset)
	return &is
}

// MatchIndexProp checks if the indexScan can match the required property.
func (p *LogicalIndexScan) MatchIndexProp(prop *property.PhysicalProperty) (match bool) {
	if prop.IsEmpty() {
		return true
	}
	if all, _ := prop.AllSameOrder(); !all {
		return false
	}
	for i, col := range p.IdxCols {
		if col.Equal(nil, prop.SortItems[0].Col) {
			return matchIndicesProp(p.IdxCols[i:], p.IdxColLens[i:], prop.SortItems)
		} else if i >= p.EqCondCount {
			break
		}
	}
	return false
}

// GetPhysicalIndexScan returns PhysicalIndexScan for the logical IndexScan.
func (s *LogicalIndexScan) GetPhysicalIndexScan(schema *expression.Schema, stats *property.StatsInfo) *PhysicalIndexScan {
	ds := s.Source
	is := PhysicalIndexScan{
		Table:            ds.tableInfo,
		TableAsName:      ds.TableAsName,
		DBName:           ds.DBName,
		Columns:          s.Columns,
		Index:            s.Index,
		IdxCols:          s.IdxCols,
		IdxColLens:       s.IdxColLens,
		Ranges:           s.Ranges,
		dataSourceSchema: ds.schema,
	}.Init(ds.ctx, ds.blockOffset)
	is.stats = stats
	is.initSchema(s.FullIdxCols, s.IsDoubleRead)
	return is
}

// BuildKeyInfo implements LogicalPlan BuildKeyInfo interface.
func (is *LogicalIndexScan) BuildKeyInfo(selfSchema *expression.Schema, childSchema []*expression.Schema) {
	selfSchema.Keys = nil
	for _, path := range is.Source.possibleAccessPaths {
		if path.IsTablePath() {
			continue
		}
		if uniqueKey, newKey := checkIndexCanBeKey(path.Index, is.Columns, selfSchema); newKey != nil {
			selfSchema.Keys = append(selfSchema.Keys, newKey)
		} else if uniqueKey != nil {
			selfSchema.UniqueKeys = append(selfSchema.UniqueKeys, uniqueKey)
		}
	}
	handle := is.getPKIsHandleCol(selfSchema)
	if handle != nil {
		selfSchema.Keys = append(selfSchema.Keys, []*expression.Column{handle})
	}
}

func (p *LogicalIndexScan) getPKIsHandleCol(schema *expression.Schema) *expression.Column {
	// We cannot use p.Source.getPKIsHandleCol() here,
	// Because we may re-prune p.Columns and p.schema during the transformation.
	// That will make p.Columns different from p.Source.Columns.
	return getPKIsHandleColFromSchema(p.Columns, schema, p.Source.tableInfo.PKIsHandle)
}

// PreparePossibleProperties implements LogicalPlan PreparePossibleProperties interface.
func (is *LogicalIndexScan) PreparePossibleProperties(schema *expression.Schema, childrenProperties ...[][]*expression.Column) [][]*expression.Column {
	if len(is.IdxCols) == 0 {
		return nil
	}
	result := make([][]*expression.Column, 0, is.EqCondCount+1)
	for i := 0; i <= is.EqCondCount; i++ {
		result = append(result, make([]*expression.Column, len(is.IdxCols)-i))
		copy(result[i], is.IdxCols[i:])
	}
	return result
}

// DeriveStats implements LogicalPlan DeriveStats interface.
func (is *LogicalIndexScan) DeriveStats(childStats []*property.StatsInfo, selfSchema *expression.Schema, childSchema []*expression.Schema, _ [][]*expression.Column) (*property.StatsInfo, error) {
	is.Source.initStats(nil)
	for i, expr := range is.AccessConds {
		is.AccessConds[i] = expression.PushDownNot(is.ctx, expr)
	}
	is.stats = is.Source.deriveStatsByFilter(is.AccessConds, nil)
	if len(is.AccessConds) == 0 {
		is.Ranges = ranger.FullRange()
	}
	is.IdxCols, is.IdxColLens = expression.IndexInfo2PrefixCols(is.Columns, selfSchema.Columns, is.Index)
	is.FullIdxCols, is.FullIdxColLens = expression.IndexInfo2Cols(is.Columns, selfSchema.Columns, is.Index)
	if !is.Index.Unique && !is.Index.Primary && len(is.Index.Columns) == len(is.IdxCols) {
		handleCol := is.getPKIsHandleCol(selfSchema)
		if handleCol != nil && !mysql.HasUnsignedFlag(handleCol.RetType.Flag) {
			is.IdxCols = append(is.IdxCols, handleCol)
			is.IdxColLens = append(is.IdxColLens, types.UnspecifiedLength)
		}
	}
	return is.stats, nil
}

func matchIndicesProp(idxCols []*expression.Column, colLens []int, propItems []property.SortItem) bool {
	if len(idxCols) < len(propItems) {
		return false
	}
	for i, item := range propItems {
		if colLens[i] != types.UnspecifiedLength || !item.Col.Equal(nil, idxCols[i]) {
			return false
		}
	}
	return true
}

func (p *LogicalIndexScan) String() string {
	builder := strings.Builder{}
	builder.WriteString("IndexScan\n")
	builder.WriteString("Schema: ")
	builder.WriteString(p.schema.String())
	builder.WriteString("\n")
	builder.WriteString(fmt.Sprintf("DataSource: %s\n", p.Source.String()))
	for _, rng := range p.Ranges {
		builder.WriteString(rng.String())
		builder.WriteString("\n")
	}
	return builder.String()
}
