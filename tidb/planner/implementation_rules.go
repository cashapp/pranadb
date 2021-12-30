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

import (
	"github.com/squareup/pranadb/tidb/planner/util"
	"math"

	"github.com/squareup/pranadb/tidb/expression"
	"github.com/squareup/pranadb/tidb/planner/property"
)

// ImplementationRule defines the interface for implementation rules.
type ImplementationRule interface {
	// Match checks if current GroupExpr matches this rule under required physical property.
	Match(expr *GroupExpr, prop *property.PhysicalProperty) (matched bool)
	// OnImplement generates physical plan using this rule for current GroupExpr. Note that
	// childrenReqProps of generated physical plan should be set correspondingly in this function.
	OnImplement(expr *GroupExpr, reqProp *property.PhysicalProperty) ([]Implementation, error)
}

var defaultImplementationMap = map[Operand][]ImplementationRule{
	OperandProjection: {
		&ImplProjection{},
	},
	OperandTableScan: {
		&ImplTableScan{},
	},
	OperandIndexScan: {
		&ImplIndexScan{},
	},
	OperandSelection: {
		&ImplSelection{},
	},
	OperandSort: {
		&ImplSort{},
	},
	OperandAggregation: {
		&ImplHashAgg{},
	},
	OperandLimit: {
		&ImplLimit{},
	},
	OperandTopN: {
		&ImplTopN{},
		&ImplTopNAsLimit{},
	},
	OperandUnionAll: {
		&ImplUnionAll{},
	},
}

// ImplProjection implements LogicalProjection as PhysicalProjection.
type ImplProjection struct {
}

// Match implements ImplementationRule Match interface.
func (r *ImplProjection) Match(expr *GroupExpr, prop *property.PhysicalProperty) (matched bool) {
	return true
}

// OnImplement implements ImplementationRule OnImplement interface.
func (r *ImplProjection) OnImplement(expr *GroupExpr, reqProp *property.PhysicalProperty) ([]Implementation, error) {
	logicProp := expr.Group.Prop
	logicProj := expr.ExprNode.(*LogicalProjection)
	childProp, ok := logicProj.TryToGetChildProp(reqProp)
	if !ok {
		return nil, nil
	}
	proj := PhysicalProjection{
		Exprs:                logicProj.Exprs,
		CalculateNoDelay:     logicProj.CalculateNoDelay,
		AvoidColumnEvaluator: logicProj.AvoidColumnEvaluator,
	}.Init(logicProj.SCtx(), logicProp.Stats.ScaleByExpectCnt(reqProp.ExpectedCnt), logicProj.SelectBlockOffset(), childProp)
	proj.SetSchema(logicProp.Schema)
	return []Implementation{NewProjectionImpl(proj)}, nil
}

// ImplTableScan implements TableScan as PhysicalTableScan.
type ImplTableScan struct {
}

// Match implements ImplementationRule Match interface.
func (r *ImplTableScan) Match(expr *GroupExpr, prop *property.PhysicalProperty) (matched bool) {
	ts := expr.ExprNode.(*LogicalTableScan)
	return prop.IsEmpty() || (len(prop.SortItems) == 1 && ts.HandleCols != nil && prop.SortItems[0].Col.Equal(nil, ts.HandleCols.GetCol(0)))
}

// OnImplement implements ImplementationRule OnImplement interface.
func (r *ImplTableScan) OnImplement(expr *GroupExpr, reqProp *property.PhysicalProperty) ([]Implementation, error) {
	logicProp := expr.Group.Prop
	logicalScan := expr.ExprNode.(*LogicalTableScan)
	ts := logicalScan.GetPhysicalScan(logicProp.Schema, logicProp.Stats.ScaleByExpectCnt(reqProp.ExpectedCnt))
	if !reqProp.IsEmpty() {
		ts.KeepOrder = true
		ts.Desc = reqProp.SortItems[0].Desc
	}
	tblCols, tblColHists := logicalScan.Source.TblCols, logicalScan.Source.TblColHists
	return []Implementation{NewTableScanImpl(ts, tblCols, tblColHists)}, nil
}

// ImplIndexScan implements IndexScan as PhysicalIndexScan.
type ImplIndexScan struct {
}

// Match implements ImplementationRule Match interface.
func (r *ImplIndexScan) Match(expr *GroupExpr, prop *property.PhysicalProperty) (matched bool) {
	is := expr.ExprNode.(*LogicalIndexScan)
	return is.MatchIndexProp(prop)
}

// OnImplement implements ImplementationRule OnImplement interface.
func (r *ImplIndexScan) OnImplement(expr *GroupExpr, reqProp *property.PhysicalProperty) ([]Implementation, error) {
	logicalScan := expr.ExprNode.(*LogicalIndexScan)
	is := logicalScan.GetPhysicalIndexScan(expr.Group.Prop.Schema, expr.Group.Prop.Stats.ScaleByExpectCnt(reqProp.ExpectedCnt))
	if !reqProp.IsEmpty() {
		is.KeepOrder = true
		if reqProp.SortItems[0].Desc {
			is.Desc = true
		}
	}
	return []Implementation{NewIndexScanImpl(is, logicalScan.Source.TblColHists)}, nil
}

// ImplSelection is the implementation rule which implements LogicalSelection
// to PhysicalSelection.
type ImplSelection struct {
}

// Match implements ImplementationRule Match interface.
func (r *ImplSelection) Match(expr *GroupExpr, prop *property.PhysicalProperty) (matched bool) {
	return true
}

// OnImplement implements ImplementationRule OnImplement interface.
func (r *ImplSelection) OnImplement(expr *GroupExpr, reqProp *property.PhysicalProperty) ([]Implementation, error) {
	logicalSel := expr.ExprNode.(*LogicalSelection)
	physicalSel := PhysicalSelection{
		Conditions: logicalSel.Conditions,
	}.Init(logicalSel.SCtx(), expr.Group.Prop.Stats.ScaleByExpectCnt(reqProp.ExpectedCnt), logicalSel.SelectBlockOffset(), reqProp.CloneEssentialFields())
	return []Implementation{NewTiDBSelectionImpl(physicalSel)}, nil
}

// ImplSort is the implementation rule which implements LogicalSort
// to PhysicalSort or NominalSort.
type ImplSort struct {
}

// Match implements ImplementationRule match interface.
func (r *ImplSort) Match(expr *GroupExpr, prop *property.PhysicalProperty) (matched bool) {
	ls := expr.ExprNode.(*LogicalSort)
	return matchItems(prop, ls.ByItems)
}

// OnImplement implements ImplementationRule OnImplement interface.
// If all of the sort items are columns, generate a NominalSort, otherwise
// generate a PhysicalSort.
func (r *ImplSort) OnImplement(expr *GroupExpr, reqProp *property.PhysicalProperty) ([]Implementation, error) {
	ls := expr.ExprNode.(*LogicalSort)
	if newProp, canUseNominal := getPropByOrderByItems(ls.ByItems); canUseNominal {
		newProp.ExpectedCnt = reqProp.ExpectedCnt
		ns := NominalSort{}.Init(
			ls.SCtx(), expr.Group.Prop.Stats.ScaleByExpectCnt(reqProp.ExpectedCnt), ls.SelectBlockOffset(), newProp)
		return []Implementation{NewNominalSortImpl(ns)}, nil
	}
	ps := PhysicalSort{ByItems: ls.ByItems}.Init(
		ls.SCtx(),
		expr.Group.Prop.Stats.ScaleByExpectCnt(reqProp.ExpectedCnt),
		ls.SelectBlockOffset(),
		&property.PhysicalProperty{ExpectedCnt: math.MaxFloat64},
	)
	return []Implementation{NewSortImpl(ps)}, nil
}

// ImplHashAgg is the implementation rule which implements LogicalAggregation
// to PhysicalHashAgg.
type ImplHashAgg struct {
}

// Match implements ImplementationRule Match interface.
func (r *ImplHashAgg) Match(expr *GroupExpr, prop *property.PhysicalProperty) (matched bool) {
	return prop.IsEmpty()
}

// OnImplement implements ImplementationRule OnImplement interface.
func (r *ImplHashAgg) OnImplement(expr *GroupExpr, reqProp *property.PhysicalProperty) ([]Implementation, error) {
	la := expr.ExprNode.(*LogicalAggregation)
	hashAgg := NewPhysicalHashAgg(
		la,
		expr.Group.Prop.Stats.ScaleByExpectCnt(reqProp.ExpectedCnt),
		&property.PhysicalProperty{ExpectedCnt: math.MaxFloat64},
	)
	hashAgg.SetSchema(expr.Group.Prop.Schema.Clone())
	return []Implementation{NewTiDBHashAggImpl(hashAgg)}, nil
}

// ImplLimit is the implementation rule which implements LogicalLimit
// to PhysicalLimit.
type ImplLimit struct {
}

// Match implements ImplementationRule Match interface.
func (r *ImplLimit) Match(expr *GroupExpr, prop *property.PhysicalProperty) (matched bool) {
	return prop.IsEmpty()
}

// OnImplement implements ImplementationRule OnImplement interface.
func (r *ImplLimit) OnImplement(expr *GroupExpr, reqProp *property.PhysicalProperty) ([]Implementation, error) {
	logicalLimit := expr.ExprNode.(*LogicalLimit)
	newProp := &property.PhysicalProperty{ExpectedCnt: float64(logicalLimit.Count + logicalLimit.Offset)}
	physicalLimit := PhysicalLimit{
		Offset: logicalLimit.Offset,
		Count:  logicalLimit.Count,
	}.Init(logicalLimit.SCtx(), expr.Group.Prop.Stats, logicalLimit.SelectBlockOffset(), newProp)
	return []Implementation{NewLimitImpl(physicalLimit)}, nil
}

// ImplTopN is the implementation rule which implements LogicalTopN
// to PhysicalTopN.
type ImplTopN struct {
}

// Match implements ImplementationRule Match interface.
func (r *ImplTopN) Match(expr *GroupExpr, prop *property.PhysicalProperty) (matched bool) {
	topN := expr.ExprNode.(*LogicalTopN)
	return matchItems(prop, topN.ByItems)
}

// OnImplement implements ImplementationRule OnImplement interface.
func (r *ImplTopN) OnImplement(expr *GroupExpr, reqProp *property.PhysicalProperty) ([]Implementation, error) {
	lt := expr.ExprNode.(*LogicalTopN)
	resultProp := &property.PhysicalProperty{ExpectedCnt: math.MaxFloat64}
	topN := PhysicalTopN{
		ByItems: lt.ByItems,
		Count:   lt.Count,
		Offset:  lt.Offset,
	}.Init(lt.SCtx(), expr.Group.Prop.Stats, lt.SelectBlockOffset(), resultProp)
	return []Implementation{NewTiDBTopNImpl(topN)}, nil
}

// ImplTopNAsLimit is the implementation rule which implements LogicalTopN
// as PhysicalLimit with required order property.
type ImplTopNAsLimit struct {
}

// Match implements ImplementationRule Match interface.
func (r *ImplTopNAsLimit) Match(expr *GroupExpr, prop *property.PhysicalProperty) (matched bool) {
	topN := expr.ExprNode.(*LogicalTopN)
	_, canUseLimit := getPropByOrderByItems(topN.ByItems)
	return canUseLimit && matchItems(prop, topN.ByItems)
}

// OnImplement implements ImplementationRule OnImplement interface.
func (r *ImplTopNAsLimit) OnImplement(expr *GroupExpr, reqProp *property.PhysicalProperty) ([]Implementation, error) {
	lt := expr.ExprNode.(*LogicalTopN)
	newProp := &property.PhysicalProperty{ExpectedCnt: float64(lt.Count + lt.Offset)}
	newProp.SortItems = make([]property.SortItem, len(lt.ByItems))
	for i, item := range lt.ByItems {
		newProp.SortItems[i].Col = item.Expr.(*expression.Column)
		newProp.SortItems[i].Desc = item.Desc
	}
	physicalLimit := PhysicalLimit{
		Offset: lt.Offset,
		Count:  lt.Count,
	}.Init(lt.SCtx(), expr.Group.Prop.Stats, lt.SelectBlockOffset(), newProp)
	return []Implementation{NewLimitImpl(physicalLimit)}, nil
}

// ImplUnionAll implements LogicalUnionAll to PhysicalUnionAll.
type ImplUnionAll struct {
}

// Match implements ImplementationRule Match interface.
func (r *ImplUnionAll) Match(expr *GroupExpr, prop *property.PhysicalProperty) (matched bool) {
	return prop.IsEmpty()
}

// OnImplement implements ImplementationRule OnImplement interface.
func (r *ImplUnionAll) OnImplement(expr *GroupExpr, reqProp *property.PhysicalProperty) ([]Implementation, error) {
	logicalUnion := expr.ExprNode.(*LogicalUnionAll)
	chReqProps := make([]*property.PhysicalProperty, len(expr.Children))
	for i := range expr.Children {
		chReqProps[i] = &property.PhysicalProperty{ExpectedCnt: reqProp.ExpectedCnt}
	}
	physicalUnion := PhysicalUnionAll{}.Init(
		logicalUnion.SCtx(),
		expr.Group.Prop.Stats.ScaleByExpectCnt(reqProp.ExpectedCnt),
		logicalUnion.SelectBlockOffset(),
		chReqProps...,
	)
	physicalUnion.SetSchema(expr.Group.Prop.Schema)
	return []Implementation{NewUnionAllImpl(physicalUnion)}, nil
}

// matchItems checks if this prop's columns can match by items totally.
func matchItems(p *property.PhysicalProperty, items []*util.ByItems) bool {
	if len(items) < len(p.SortItems) {
		return false
	}
	for i, col := range p.SortItems {
		sortItem := items[i]
		if sortItem.Desc != col.Desc || !sortItem.Expr.Equal(nil, col.Col) {
			return false
		}
	}
	return true
}

// getPropByOrderByItems will check if this sort property can be pushed or not. In order to simplify the problem, we only
// consider the case that all expression are columns.
func getPropByOrderByItems(items []*util.ByItems) (*property.PhysicalProperty, bool) {
	propItems := make([]property.SortItem, 0, len(items))
	for _, item := range items {
		col, ok := item.Expr.(*expression.Column)
		if !ok {
			return nil, false
		}
		propItems = append(propItems, property.SortItem{Col: col, Desc: item.Desc})
	}
	return &property.PhysicalProperty{SortItems: propItems}, true
}
