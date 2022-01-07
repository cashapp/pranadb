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
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/squareup/pranadb/tidb/expression"
	"github.com/squareup/pranadb/tidb/infoschema"
	"github.com/squareup/pranadb/tidb/planner/property"
	"github.com/squareup/pranadb/tidb/planner/util"
	"github.com/squareup/pranadb/tidb/sessionctx"
	"github.com/squareup/pranadb/tidb/statistics"
	"github.com/squareup/pranadb/tidb/types"
	"github.com/squareup/pranadb/tidb/util/collate"
	"github.com/squareup/pranadb/tidb/util/logutil"
	"github.com/squareup/pranadb/tidb/util/ranger"
	"go.uber.org/zap"
	"math"
	"sort"
	"strings"
)

// LogicalDataSource represents a tableScan without condition push down.
type LogicalDataSource struct {
	logicalSchemaProducer

	tableInfo *model.TableInfo
	Columns   []*model.ColumnInfo
	DBName    model.CIStr

	TableAsName *model.CIStr
	// pushedDownConds are the conditions that will be pushed down to coprocessor.
	pushedDownConds []expression.Expression
	// allConds contains all the filters on this table. For now it's maintained
	// in predicate push down and used only in partition pruning.
	allConds []expression.Expression

	statisticTable *statistics.Table
	tableStats     *property.StatsInfo

	// possibleAccessPaths stores all the possible access path for physical plan, including table scan.
	possibleAccessPaths []*util.AccessPath

	// handleCol represents the handle column for the datasource, either the
	// int primary key column or extra handle column.
	// handleCol *expression.Column
	handleCols HandleCols
	// TblCols contains the original columns of table before being pruned, and it
	// is used for estimating table scan cost.
	TblCols []*expression.Column
	// commonHandleCols and commonHandleLens save the info of primary key which is the clustered index.
	commonHandleCols []*expression.Column
	commonHandleLens []int
	// TblColHists contains the Histogram of all original table columns,
	// it is converted from statisticTable, and used for IO/network cost estimating.
	TblColHists *statistics.HistColl

	is infoschema.InfoSchema
}

// Init initializes LogicalDataSource.
func (ds LogicalDataSource) Init(ctx sessionctx.Context, offset int) *LogicalDataSource {
	ds.baseLogicalPlan = newBaseLogicalPlan(ctx, TypeDataSource, &ds, offset)
	return &ds
}

// BuildKeyInfo implements LogicalPlan BuildKeyInfo interface.
func (ds *LogicalDataSource) BuildKeyInfo(selfSchema *expression.Schema, childSchema []*expression.Schema) {
	selfSchema.Keys = nil
	for _, path := range ds.possibleAccessPaths {
		if path.IsIntHandlePath {
			continue
		}
		if uniqueKey, newKey := checkIndexCanBeKey(path.Index, ds.Columns, selfSchema); newKey != nil {
			selfSchema.Keys = append(selfSchema.Keys, newKey)
		} else if uniqueKey != nil {
			selfSchema.UniqueKeys = append(selfSchema.UniqueKeys, uniqueKey)
		}
	}
	if ds.tableInfo.PKIsHandle {
		for i, col := range ds.Columns {
			if mysql.HasPriKeyFlag(col.Flag) {
				selfSchema.Keys = append(selfSchema.Keys, []*expression.Column{selfSchema.Columns[i]})
				break
			}
		}
	}
}

func (ds *LogicalDataSource) buildTableScan() LogicalPlan {
	ts := LogicalTableScan{Source: ds, HandleCols: ds.handleCols}.Init(ds.ctx, ds.blockOffset)
	ts.SetSchema(ds.Schema())
	return ts
}

func (ds *LogicalDataSource) buildIndexScan(path *util.AccessPath) LogicalPlan {
	is := LogicalIndexScan{
		Source:         ds,
		IsDoubleRead:   false,
		Index:          path.Index,
		FullIdxCols:    path.FullIdxCols,
		FullIdxColLens: path.FullIdxColLens,
		IdxCols:        path.IdxCols,
		IdxColLens:     path.IdxColLens,
	}.Init(ds.ctx, ds.blockOffset)

	is.Columns = make([]*model.ColumnInfo, len(ds.Columns))
	copy(is.Columns, ds.Columns)
	is.SetSchema(ds.Schema())
	is.IdxCols, is.IdxColLens = expression.IndexInfo2PrefixCols(is.Columns, is.schema.Columns, is.Index)
	return is
}

func (ds *LogicalDataSource) Convert2Scans() (scans []LogicalPlan) {
	scan := ds.buildTableScan()
	scans = append(scans, scan)
	for _, path := range ds.possibleAccessPaths {
		if !path.IsIntHandlePath {
			path.FullIdxCols, path.FullIdxColLens = expression.IndexInfo2Cols(ds.Columns, ds.schema.Columns, path.Index)
			path.IdxCols, path.IdxColLens = expression.IndexInfo2PrefixCols(ds.Columns, ds.schema.Columns, path.Index)
			// If index columns can cover all of the needed columns, we can use a IndexGather + IndexScan.
			if ds.isCoveringIndex(ds.schema.Columns, path.FullIdxCols, path.FullIdxColLens, ds.tableInfo) {
				scans = append(scans, ds.buildIndexScan(path))
			}
			// TODO: If index columns can not cover the schema, use IndexLookUpGather.
		}
	}
	return scans
}

func (ds *LogicalDataSource) deriveCommonHandleTablePathStats(path *util.AccessPath, conds []expression.Expression, isIm bool) (bool, error) {
	path.CountAfterAccess = float64(ds.statisticTable.Count)
	path.Ranges = ranger.FullNotNullRange()
	path.IdxCols, path.IdxColLens = expression.IndexInfo2PrefixCols(ds.Columns, ds.schema.Columns, path.Index)
	path.FullIdxCols, path.FullIdxColLens = expression.IndexInfo2Cols(ds.Columns, ds.schema.Columns, path.Index)
	if len(conds) == 0 {
		return false, nil
	}
	sc := ds.ctx.GetSessionVars().StmtCtx
	if len(path.IdxCols) != 0 {
		res, err := ranger.DetachCondAndBuildRangeForIndex(ds.ctx, conds, path.IdxCols, path.IdxColLens)
		if err != nil {
			return false, err
		}
		path.Ranges = res.Ranges
		path.AccessConds = res.AccessConds
		path.TableFilters = res.RemainedConds
		path.EqCondCount = res.EqCondCount
		path.EqOrInCondCount = res.EqOrInCount
		path.IsDNFCond = res.IsDNFCond
		path.CountAfterAccess, err = ds.tableStats.HistColl.GetRowCountByIndexRanges(sc, path.Index.ID, path.Ranges)
		if err != nil {
			return false, err
		}
	} else {
		path.TableFilters = conds
	}
	if path.EqOrInCondCount == len(path.AccessConds) {
		accesses, remained := path.SplitCorColAccessCondFromFilters(ds.ctx, path.EqOrInCondCount)
		path.AccessConds = append(path.AccessConds, accesses...)
		path.TableFilters = remained
		if len(accesses) > 0 && ds.statisticTable.Pseudo {
			path.CountAfterAccess = ds.statisticTable.PseudoAvgCountPerValue()
		} else {
			selectivity := path.CountAfterAccess / float64(ds.statisticTable.Count)
			for i := range accesses {
				col := path.IdxCols[path.EqOrInCondCount+i]
				ndv := ds.getColumnNDV(col.ID)
				ndv *= selectivity
				if ndv < 1 {
					ndv = 1.0
				}
				path.CountAfterAccess = path.CountAfterAccess / ndv
			}
		}
	}
	// If the `CountAfterAccess` is less than `stats.RowCount`, there must be some inconsistent stats info.
	// We prefer the `stats.RowCount` because it could use more stats info to calculate the selectivity.
	if path.CountAfterAccess < ds.stats.RowCount && !isIm {
		path.CountAfterAccess = math.Min(ds.stats.RowCount/SelectionFactor, float64(ds.statisticTable.Count))
	}
	// Check whether there's only point query.
	noIntervalRanges := true
	haveNullVal := false
	for _, ran := range path.Ranges {
		// Not point or the not full matched.
		if !ran.IsPoint(sc) || len(ran.HighVal) != len(path.Index.Columns) {
			noIntervalRanges = false
			break
		}
		// Check whether there's null value.
		for i := 0; i < len(path.Index.Columns); i++ {
			if ran.HighVal[i].IsNull() {
				haveNullVal = true
				break
			}
		}
		if haveNullVal {
			break
		}
	}
	return noIntervalRanges && !haveNullVal, nil
}

// deriveTablePathStats will fulfill the information that the AccessPath need.
// And it will check whether the primary key is covered only by point query.
// isIm indicates whether this function is called to generate the partial path for IndexMerge.
func (ds *LogicalDataSource) deriveTablePathStats(path *util.AccessPath, conds []expression.Expression, isIm bool) (bool, error) {
	if path.IsCommonHandlePath {
		return ds.deriveCommonHandleTablePathStats(path, conds, isIm)
	}
	var err error
	sc := ds.ctx.GetSessionVars().StmtCtx
	path.CountAfterAccess = float64(ds.statisticTable.Count)
	path.TableFilters = conds
	var pkCol *expression.Column
	columnLen := len(ds.schema.Columns)
	isUnsigned := false
	if ds.tableInfo.PKIsHandle {
		if pkColInfo := ds.tableInfo.GetPkColInfo(); pkColInfo != nil {
			isUnsigned = mysql.HasUnsignedFlag(pkColInfo.Flag)
			pkCol = expression.ColInfo2Col(ds.schema.Columns, pkColInfo)
		}
	} else if columnLen > 0 && ds.schema.Columns[columnLen-1].ID == model.ExtraHandleID {
		pkCol = ds.schema.Columns[columnLen-1]
	}
	if pkCol == nil {
		path.Ranges = ranger.FullIntRange(isUnsigned)
		return false, nil
	}

	path.Ranges = ranger.FullIntRange(isUnsigned)
	if len(conds) == 0 {
		return false, nil
	}
	path.AccessConds, path.TableFilters = ranger.DetachCondsForColumn(ds.ctx, conds, pkCol)
	// If there's no access cond, we try to find that whether there's expression containing correlated column that
	// can be used to access data.
	corColInAccessConds := false
	if len(path.AccessConds) == 0 {
		for i, filter := range path.TableFilters {
			eqFunc, ok := filter.(*expression.ScalarFunction)
			if !ok || eqFunc.FuncName.L != ast.EQ {
				continue
			}
			lCol, lOk := eqFunc.GetArgs()[0].(*expression.Column)
			if lOk && lCol.Equal(ds.ctx, pkCol) {
				_, rOk := eqFunc.GetArgs()[1].(*expression.CorrelatedColumn)
				if rOk {
					path.AccessConds = append(path.AccessConds, filter)
					path.TableFilters = append(path.TableFilters[:i], path.TableFilters[i+1:]...)
					corColInAccessConds = true
					break
				}
			}
			rCol, rOk := eqFunc.GetArgs()[1].(*expression.Column)
			if rOk && rCol.Equal(ds.ctx, pkCol) {
				_, lOk := eqFunc.GetArgs()[0].(*expression.CorrelatedColumn)
				if lOk {
					path.AccessConds = append(path.AccessConds, filter)
					path.TableFilters = append(path.TableFilters[:i], path.TableFilters[i+1:]...)
					corColInAccessConds = true
					break
				}
			}
		}
	}
	if corColInAccessConds {
		path.CountAfterAccess = 1
		return true, nil
	}
	path.Ranges, err = ranger.BuildTableRange(path.AccessConds, sc, pkCol.RetType)
	if err != nil {
		return false, err
	}
	path.CountAfterAccess, err = ds.statisticTable.GetRowCountByIntColumnRanges(sc, pkCol.ID, path.Ranges)
	// If the `CountAfterAccess` is less than `stats.RowCount`, there must be some inconsistent stats info.
	// We prefer the `stats.RowCount` because it could use more stats info to calculate the selectivity.
	if path.CountAfterAccess < ds.stats.RowCount && !isIm {
		path.CountAfterAccess = math.Min(ds.stats.RowCount/SelectionFactor, float64(ds.statisticTable.Count))
	}
	// Check whether the primary key is covered by point query.
	noIntervalRange := true
	for _, ran := range path.Ranges {
		if !ran.IsPoint(sc) {
			noIntervalRange = false
			break
		}
	}
	return noIntervalRange, err
}

func (ds *LogicalDataSource) fillIndexPath(path *util.AccessPath, conds []expression.Expression) error {
	sc := ds.ctx.GetSessionVars().StmtCtx
	path.Ranges = ranger.FullRange()
	path.CountAfterAccess = float64(ds.statisticTable.Count)
	path.IdxCols, path.IdxColLens = expression.IndexInfo2PrefixCols(ds.Columns, ds.schema.Columns, path.Index)
	path.FullIdxCols, path.FullIdxColLens = expression.IndexInfo2Cols(ds.Columns, ds.schema.Columns, path.Index)
	if !path.Index.Unique && !path.Index.Primary && len(path.Index.Columns) == len(path.IdxCols) {
		handleCol := ds.getPKIsHandleCol()
		if handleCol != nil && !mysql.HasUnsignedFlag(handleCol.RetType.Flag) {
			alreadyHandle := false
			for _, col := range path.IdxCols {
				if col.ID == model.ExtraHandleID || col.Equal(nil, handleCol) {
					alreadyHandle = true
				}
			}
			// Don't add one column twice to the index. May cause unexpected errors.
			if !alreadyHandle {
				path.IdxCols = append(path.IdxCols, handleCol)
				path.IdxColLens = append(path.IdxColLens, types.UnspecifiedLength)
				// Also updates the map that maps the index id to its prefix column ids.
				if len(ds.tableStats.HistColl.Idx2ColumnIDs[path.Index.ID]) == len(path.Index.Columns) {
					ds.tableStats.HistColl.Idx2ColumnIDs[path.Index.ID] = append(ds.tableStats.HistColl.Idx2ColumnIDs[path.Index.ID], handleCol.UniqueID)
				}
			}
		}
	}
	if len(path.IdxCols) != 0 {
		res, err := ranger.DetachCondAndBuildRangeForIndex(ds.ctx, conds, path.IdxCols, path.IdxColLens)
		if err != nil {
			return err
		}
		path.Ranges = res.Ranges
		path.AccessConds = res.AccessConds
		path.TableFilters = res.RemainedConds
		path.EqCondCount = res.EqCondCount
		path.EqOrInCondCount = res.EqOrInCount
		path.IsDNFCond = res.IsDNFCond
		path.CountAfterAccess, err = ds.tableStats.HistColl.GetRowCountByIndexRanges(sc, path.Index.ID, path.Ranges)
		if err != nil {
			return err
		}
	} else {
		path.TableFilters = conds
	}
	return nil
}

// deriveIndexPathStats will fulfill the information that the AccessPath need.
// And it will check whether this index is full matched by point query. We will use this check to
// determine whether we remove other paths or not.
// conds is the conditions used to generate the DetachRangeResult for path.
// isIm indicates whether this function is called to generate the partial path for IndexMerge.
func (ds *LogicalDataSource) deriveIndexPathStats(path *util.AccessPath, conds []expression.Expression, isIm bool) bool {
	sc := ds.ctx.GetSessionVars().StmtCtx
	if path.EqOrInCondCount == len(path.AccessConds) {
		accesses, remained := path.SplitCorColAccessCondFromFilters(ds.ctx, path.EqOrInCondCount)
		path.AccessConds = append(path.AccessConds, accesses...)
		path.TableFilters = remained
		if len(accesses) > 0 && ds.statisticTable.Pseudo {
			path.CountAfterAccess = ds.statisticTable.PseudoAvgCountPerValue()
		} else {
			selectivity := path.CountAfterAccess / float64(ds.statisticTable.Count)
			for i := range accesses {
				col := path.IdxCols[path.EqOrInCondCount+i]
				ndv := ds.getColumnNDV(col.ID)
				ndv *= selectivity
				if ndv < 1 {
					ndv = 1.0
				}
				path.CountAfterAccess = path.CountAfterAccess / ndv
			}
		}
	}
	var indexFilters []expression.Expression
	indexFilters, path.TableFilters = ds.splitIndexFilterConditions(path.TableFilters, path.FullIdxCols, path.FullIdxColLens, ds.tableInfo)
	path.IndexFilters = append(path.IndexFilters, indexFilters...)
	// If the `CountAfterAccess` is less than `stats.RowCount`, there must be some inconsistent stats info.
	// We prefer the `stats.RowCount` because it could use more stats info to calculate the selectivity.
	if path.CountAfterAccess < ds.stats.RowCount && !isIm {
		path.CountAfterAccess = math.Min(ds.stats.RowCount/SelectionFactor, float64(ds.statisticTable.Count))
	}
	if path.IndexFilters != nil {
		selectivity, _, err := ds.tableStats.HistColl.Selectivity(ds.ctx, path.IndexFilters, nil)
		if err != nil {
			logutil.BgLogger().Debug("calculate selectivity failed, use selection factor", zap.Error(err))
			selectivity = SelectionFactor
		}
		if isIm {
			path.CountAfterIndex = path.CountAfterAccess * selectivity
		} else {
			path.CountAfterIndex = math.Max(path.CountAfterAccess*selectivity, ds.stats.RowCount)
		}
	}
	// Check whether there's only point query.
	noIntervalRanges := true
	haveNullVal := false
	for _, ran := range path.Ranges {
		// Not point or the not full matched.
		if !ran.IsPoint(sc) || len(ran.HighVal) != len(path.Index.Columns) {
			noIntervalRanges = false
			break
		}
		// Check whether there's null value.
		for i := 0; i < len(path.Index.Columns); i++ {
			if ran.HighVal[i].IsNull() {
				haveNullVal = true
				break
			}
		}
		if haveNullVal {
			break
		}
	}
	return noIntervalRanges && !haveNullVal
}

func getPKIsHandleColFromSchema(cols []*model.ColumnInfo, schema *expression.Schema, pkIsHandle bool) *expression.Column {
	if !pkIsHandle {
		// If the PKIsHandle is false, return the ExtraHandleColumn.
		for i, col := range cols {
			if col.ID == model.ExtraHandleID {
				return schema.Columns[i]
			}
		}
		return nil
	}
	for i, col := range cols {
		if mysql.HasPriKeyFlag(col.Flag) {
			return schema.Columns[i]
		}
	}
	return nil
}

func (ds *LogicalDataSource) getPKIsHandleCol() *expression.Column {
	return getPKIsHandleColFromSchema(ds.Columns, ds.schema, ds.tableInfo.PKIsHandle)
}

func (ds *LogicalDataSource) isCoveringIndex(columns, indexColumns []*expression.Column, idxColLens []int, tblInfo *model.TableInfo) bool {
	for _, col := range columns {
		if tblInfo.PKIsHandle && mysql.HasPriKeyFlag(col.RetType.Flag) {
			continue
		}
		if col.ID == model.ExtraHandleID {
			continue
		}
		coveredByPlainIndex := indexCoveringCol(col, indexColumns, idxColLens)
		coveredByClusteredIndex := indexCoveringCol(col, ds.commonHandleCols, ds.commonHandleLens)
		if !coveredByPlainIndex && !coveredByClusteredIndex {
			return false
		}
		isClusteredNewCollationIdx := collate.NewCollationEnabled() &&
			col.GetType().EvalType() == types.ETString &&
			!mysql.HasBinaryFlag(col.GetType().Flag)
		if !coveredByPlainIndex && coveredByClusteredIndex && isClusteredNewCollationIdx && ds.tableInfo.CommonHandleVersion == 0 {
			return false
		}
	}
	return true
}

func indexCoveringCol(col *expression.Column, indexCols []*expression.Column, idxColLens []int) bool {
	for i, indexCol := range indexCols {
		isFullLen := idxColLens[i] == types.UnspecifiedLength || idxColLens[i] == col.RetType.Flen
		if indexCol != nil && col.Equal(nil, indexCol) && isFullLen {
			return true
		}
	}
	return false
}

func (ds *LogicalDataSource) splitIndexFilterConditions(conditions []expression.Expression, indexColumns []*expression.Column, idxColLens []int,
	table *model.TableInfo) (indexConds, tableConds []expression.Expression) {
	var indexConditions, tableConditions []expression.Expression
	for _, cond := range conditions {
		if ds.isCoveringIndex(expression.ExtractColumns(cond), indexColumns, idxColLens, table) {
			indexConditions = append(indexConditions, cond)
		} else {
			tableConditions = append(tableConditions, cond)
		}
	}
	return indexConditions, tableConditions
}

// TableInfo returns the *TableInfo of data source.
func (ds *LogicalDataSource) TableInfo() *model.TableInfo {
	return ds.tableInfo
}

// PreparePossibleProperties implements LogicalPlan PreparePossibleProperties interface.
func (ds *LogicalDataSource) PreparePossibleProperties(schema *expression.Schema, childrenProperties ...[][]*expression.Column) [][]*expression.Column {
	result := make([][]*expression.Column, 0, len(ds.possibleAccessPaths))

	for _, path := range ds.possibleAccessPaths {
		if path.IsIntHandlePath {
			col := ds.getPKIsHandleCol()
			if col != nil {
				result = append(result, []*expression.Column{col})
			}
			continue
		}

		if len(path.IdxCols) == 0 {
			continue
		}
		result = append(result, make([]*expression.Column, len(path.IdxCols)))
		copy(result[len(result)-1], path.IdxCols)
		for i := 0; i < path.EqCondCount && i+1 < len(path.IdxCols); i++ {
			result = append(result, make([]*expression.Column, len(path.IdxCols)-i-1))
			copy(result[len(result)-1], path.IdxCols[i+1:])
		}
	}
	return result
}

func (ds *LogicalDataSource) newExtraHandleSchemaCol() *expression.Column {
	tp := types.NewFieldType(mysql.TypeLonglong)
	tp.Flag = mysql.NotNullFlag | mysql.PriKeyFlag
	return &expression.Column{
		RetType:  tp,
		UniqueID: ds.ctx.GetSessionVars().AllocPlanColumnID(),
		ID:       model.ExtraHandleID,
		OrigName: fmt.Sprintf("%v.%v.%v", ds.DBName, ds.tableInfo.Name, model.ExtraHandleName),
	}
}

// getColumnNDV computes estimated NDV of specified column using the original
// histogram of `LogicalDataSource` which is retrieved from storage(not the derived one).
func (ds *LogicalDataSource) getColumnNDV(colID int64) (ndv float64) {
	hist, ok := ds.statisticTable.Columns[colID]
	if ok && hist.Count > 0 {
		factor := float64(ds.statisticTable.Count) / float64(hist.Count)
		ndv = float64(hist.Histogram.NDV) * factor
	} else {
		ndv = float64(ds.statisticTable.Count) * distinctFactor
	}
	return ndv
}

func (ds *LogicalDataSource) getGroupNDVs(colGroups [][]*expression.Column) []property.GroupNDV {
	if colGroups == nil {
		return nil
	}
	tbl := ds.tableStats.HistColl
	ndvs := make([]property.GroupNDV, 0, len(colGroups))
	for idxID, idx := range tbl.Indices {
		colsLen := len(tbl.Idx2ColumnIDs[idxID])
		// tbl.Idx2ColumnIDs may only contain the prefix of index columns.
		// But it may exceeds the total index since the index would contain the handle column if it's not a unique index.
		// We append the handle at fillIndexPath.
		if colsLen < len(idx.Info.Columns) {
			continue
		} else if colsLen > len(idx.Info.Columns) {
			colsLen--
		}
		idxCols := make([]int64, colsLen)
		copy(idxCols, tbl.Idx2ColumnIDs[idxID])
		sort.Slice(idxCols, func(i, j int) bool {
			return idxCols[i] < idxCols[j]
		})
		for _, g := range colGroups {
			// We only want those exact matches.
			if len(g) != colsLen {
				continue
			}
			match := true
			for i, col := range g {
				// Both slices are sorted according to UniqueID.
				if col.UniqueID != idxCols[i] {
					match = false
					break
				}
			}
			if match {
				ndv := property.GroupNDV{
					Cols: idxCols,
					NDV:  float64(idx.NDV),
				}
				ndvs = append(ndvs, ndv)
				break
			}
		}
	}
	return ndvs
}

func (ds *LogicalDataSource) initStats(colGroups [][]*expression.Column) {
	if ds.tableStats != nil {
		// Reload GroupNDVs since colGroups may have changed.
		ds.tableStats.GroupNDVs = ds.getGroupNDVs(colGroups)
		return
	}
	if ds.statisticTable == nil {
		ds.statisticTable = getStatsTable(ds.tableInfo)
	}
	tableStats := &property.StatsInfo{
		RowCount:     float64(ds.statisticTable.Count),
		ColNDVs:      make(map[int64]float64, ds.schema.Len()),
		HistColl:     ds.statisticTable.GenerateHistCollFromColumnInfo(ds.Columns, ds.schema.Columns),
		StatsVersion: ds.statisticTable.Version,
	}
	if ds.statisticTable.Pseudo {
		tableStats.StatsVersion = statistics.PseudoVersion
	}
	for _, col := range ds.schema.Columns {
		tableStats.ColNDVs[col.UniqueID] = ds.getColumnNDV(col.ID)
	}
	ds.tableStats = tableStats
	ds.tableStats.GroupNDVs = ds.getGroupNDVs(colGroups)
	ds.TblColHists = ds.statisticTable.ID2UniqueID(ds.TblCols)
}

func (ds *LogicalDataSource) deriveStatsByFilter(conds expression.CNFExprs, filledPaths []*util.AccessPath) *property.StatsInfo {
	selectivity, nodes, err := ds.tableStats.HistColl.Selectivity(ds.ctx, conds, filledPaths)
	if err != nil {
		logutil.BgLogger().Debug("something wrong happened, use the default selectivity", zap.Error(err))
		selectivity = SelectionFactor
	}
	stats := ds.tableStats.Scale(selectivity)
	stats.HistColl = stats.HistColl.NewHistCollBySelectivity(ds.ctx.GetSessionVars().StmtCtx, nodes)
	return stats
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (ds *LogicalDataSource) DeriveStats(childStats []*property.StatsInfo, selfSchema *expression.Schema, childSchema []*expression.Schema, colGroups [][]*expression.Column) (*property.StatsInfo, error) {
	if ds.stats != nil && len(colGroups) == 0 {
		return ds.stats, nil
	}
	ds.initStats(colGroups)
	if ds.stats != nil {
		// Just reload the GroupNDVs.
		selectivity := ds.stats.RowCount / ds.tableStats.RowCount
		ds.stats = ds.tableStats.Scale(selectivity)
		return ds.stats, nil
	}
	// PushDownNot here can convert query 'not (a != 1)' to 'a = 1'.
	for i, expr := range ds.pushedDownConds {
		ds.pushedDownConds[i] = expression.PushDownNot(ds.ctx, expr)
	}
	for _, path := range ds.possibleAccessPaths {
		if path.IsTablePath() {
			continue
		}
		err := ds.fillIndexPath(path, ds.pushedDownConds)
		if err != nil {
			return nil, err
		}
	}
	ds.stats = ds.deriveStatsByFilter(ds.pushedDownConds, ds.possibleAccessPaths)
	for _, path := range ds.possibleAccessPaths {
		if path.IsTablePath() {
			noIntervalRanges, err := ds.deriveTablePathStats(path, ds.pushedDownConds, false)
			if err != nil {
				return nil, err
			}
			// If we have point or empty range, just remove other possible paths.
			if noIntervalRanges || len(path.Ranges) == 0 {
				ds.possibleAccessPaths[0] = path
				ds.possibleAccessPaths = ds.possibleAccessPaths[:1]
				ds.ctx.GetSessionVars().StmtCtx.OptimDependOnMutableConst = true
				break
			}
			continue
		}
		noIntervalRanges := ds.deriveIndexPathStats(path, ds.pushedDownConds, false)
		// If we have empty range, or point range on unique index, just remove other possible paths.
		if (noIntervalRanges && path.Index.Unique) || len(path.Ranges) == 0 {
			ds.possibleAccessPaths[0] = path
			ds.possibleAccessPaths = ds.possibleAccessPaths[:1]
			ds.ctx.GetSessionVars().StmtCtx.OptimDependOnMutableConst = true
			break
		}
	}

	return ds.stats, nil
}

// getIndexMergeOrPath generates all possible IndexMergeOrPaths.
func (ds *LogicalDataSource) generateIndexMergeOrPaths() error {
	usedIndexCount := len(ds.possibleAccessPaths)
	for i, cond := range ds.pushedDownConds {
		sf, ok := cond.(*expression.ScalarFunction)
		if !ok || sf.FuncName.L != ast.LogicOr {
			continue
		}
		var partialPaths = make([]*util.AccessPath, 0, usedIndexCount)
		dnfItems := expression.FlattenDNFConditions(sf)
		for _, item := range dnfItems {
			cnfItems := expression.SplitCNFItems(item)
			itemPaths := ds.accessPathsForConds(cnfItems, usedIndexCount)
			if len(itemPaths) == 0 {
				partialPaths = nil
				break
			}
			partialPath, err := ds.buildIndexMergePartialPath(itemPaths)
			if err != nil {
				return err
			}
			if partialPath == nil {
				partialPaths = nil
				break
			}
			partialPaths = append(partialPaths, partialPath)
		}
		// If all of the partialPaths use the same index, we will not use the indexMerge.
		singlePath := true
		for i := len(partialPaths) - 1; i >= 1; i-- {
			if partialPaths[i].Index != partialPaths[i-1].Index {
				singlePath = false
				break
			}
		}
		if singlePath {
			continue
		}
		if len(partialPaths) > 1 {
			possiblePath := ds.buildIndexMergeOrPath(partialPaths, i)
			if possiblePath == nil {
				return nil
			}

			accessConds := make([]expression.Expression, 0, len(partialPaths))
			for _, p := range partialPaths {
				indexCondsForP := p.AccessConds[:]
				indexCondsForP = append(indexCondsForP, p.IndexFilters...)
				if len(indexCondsForP) > 0 {
					accessConds = append(accessConds, expression.ComposeCNFCondition(ds.ctx, indexCondsForP...))
				}
			}
			accessDNF := expression.ComposeDNFCondition(ds.ctx, accessConds...)
			sel, _, err := ds.tableStats.HistColl.Selectivity(ds.ctx, []expression.Expression{accessDNF}, nil)
			if err != nil {
				logutil.BgLogger().Debug("something wrong happened, use the default selectivity", zap.Error(err))
				sel = SelectionFactor
			}
			possiblePath.CountAfterAccess = sel * ds.tableStats.RowCount
			ds.possibleAccessPaths = append(ds.possibleAccessPaths, possiblePath)
		}
	}
	return nil
}

// accessPathsForConds generates all possible index paths for conditions.
func (ds *LogicalDataSource) accessPathsForConds(conditions []expression.Expression, usedIndexCount int) []*util.AccessPath {
	var results = make([]*util.AccessPath, 0, usedIndexCount)
	for i := 0; i < usedIndexCount; i++ {
		path := &util.AccessPath{}
		if ds.possibleAccessPaths[i].IsTablePath() {
			if ds.tableInfo.IsCommonHandle {
				path.IsCommonHandlePath = true
				path.Index = ds.possibleAccessPaths[i].Index
			} else {
				path.IsIntHandlePath = true
			}
			noIntervalRanges, err := ds.deriveTablePathStats(path, conditions, true)
			if err != nil {
				logutil.BgLogger().Debug("can not derive statistics of a path", zap.Error(err))
				continue
			}
			// If the path contains a full range, ignore it.
			if ranger.HasFullRange(path.Ranges) {
				continue
			}
			// If we have point or empty range, just remove other possible paths.
			if noIntervalRanges || len(path.Ranges) == 0 {
				if len(results) == 0 {
					results = append(results, path)
				} else {
					results[0] = path
					results = results[:1]
				}
				ds.ctx.GetSessionVars().StmtCtx.OptimDependOnMutableConst = true
				break
			}
		} else {
			path.Index = ds.possibleAccessPaths[i].Index
			err := ds.fillIndexPath(path, conditions)
			if err != nil {
				logutil.BgLogger().Debug("can not derive statistics of a path", zap.Error(err))
				continue
			}
			noIntervalRanges := ds.deriveIndexPathStats(path, conditions, true)
			// If the path contains a full range, ignore it.
			if ranger.HasFullRange(path.Ranges) {
				continue
			}
			// If we have empty range, or point range on unique index, just remove other possible paths.
			if (noIntervalRanges && path.Index.Unique) || len(path.Ranges) == 0 {
				if len(results) == 0 {
					results = append(results, path)
				} else {
					results[0] = path
					results = results[:1]
				}
				ds.ctx.GetSessionVars().StmtCtx.OptimDependOnMutableConst = true
				break
			}
		}
		results = append(results, path)
	}
	return results
}

// buildIndexMergePartialPath chooses the best index path from all possible paths.
// Now we choose the index with minimal estimate row count.
func (ds *LogicalDataSource) buildIndexMergePartialPath(indexAccessPaths []*util.AccessPath) (*util.AccessPath, error) {
	if len(indexAccessPaths) == 1 {
		return indexAccessPaths[0], nil
	}

	minEstRowIndex := 0
	minEstRow := math.MaxFloat64
	for i := 0; i < len(indexAccessPaths); i++ {
		rc := indexAccessPaths[i].CountAfterAccess
		if len(indexAccessPaths[i].IndexFilters) > 0 {
			rc = indexAccessPaths[i].CountAfterIndex
		}
		if rc < minEstRow {
			minEstRowIndex = i
			minEstRow = rc
		}
	}
	return indexAccessPaths[minEstRowIndex], nil
}

// buildIndexMergeOrPath generates one possible IndexMergePath.
func (ds *LogicalDataSource) buildIndexMergeOrPath(partialPaths []*util.AccessPath, current int) *util.AccessPath {
	indexMergePath := &util.AccessPath{PartialIndexPaths: partialPaths}
	indexMergePath.TableFilters = append(indexMergePath.TableFilters, ds.pushedDownConds[:current]...)
	indexMergePath.TableFilters = append(indexMergePath.TableFilters, ds.pushedDownConds[current+1:]...)
	for _, path := range partialPaths {
		// If any partial path contains table filters, we need to keep the whole DNF filter in the Selection.
		if len(path.TableFilters) > 0 {
			indexMergePath.TableFilters = append(indexMergePath.TableFilters, ds.pushedDownConds[current])
			break
		}
	}
	return indexMergePath
}

// PruneColumns implements LogicalPlan interface.
func (ds *LogicalDataSource) PruneColumns(parentUsedCols []*expression.Column) error {
	used := expression.GetUsedList(parentUsedCols, ds.schema)

	exprCols := expression.ExtractColumnsFromExpressions(nil, ds.allConds, nil)
	exprUsed := expression.GetUsedList(exprCols, ds.schema)

	for i := len(used) - 1; i >= 0; i-- {
		if !used[i] && !exprUsed[i] {
			ds.schema.Columns = append(ds.schema.Columns[:i], ds.schema.Columns[i+1:]...)
			ds.Columns = append(ds.Columns[:i], ds.Columns[i+1:]...)
		}
	}
	// For SQL like `select 1 from t`, tikv's response will be empty if no column is in schema.
	// So we'll force to push one if schema doesn't have any column.
	if ds.schema.Len() == 0 {
		var handleCol *expression.Column
		var handleColInfo *model.ColumnInfo
		if ds.handleCols != nil {
			handleCol = ds.handleCols.GetCol(0)
			handleColInfo = handleCol.ToInfo()
		} else {
			handleCol = ds.newExtraHandleSchemaCol()
			handleColInfo = model.NewExtraHandleColInfo()
		}
		ds.Columns = append(ds.Columns, handleColInfo)
		ds.schema.Append(handleCol)
	}
	if ds.handleCols != nil && ds.handleCols.IsInt() && ds.schema.ColumnIndex(ds.handleCols.GetCol(0)) == -1 {
		ds.handleCols = nil
	}
	return nil
}

func (p *LogicalDataSource) String() string {
	builder := strings.Builder{}
	builder.WriteString("DataSource\n")
	builder.WriteString("Schema: ")
	builder.WriteString(p.schema.String())
	return builder.String()
}
