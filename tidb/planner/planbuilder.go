//
// This source code is a modified form of original source from the TiDB project, which has the following copyright header(s):
//

// Copyright 2015 PingCAP, Inc.
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
	"context"
	"github.com/cznic/mathutil"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/opcode"
	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/tidb"
	"github.com/squareup/pranadb/tidb/expression"
	"github.com/squareup/pranadb/tidb/infoschema"
	"github.com/squareup/pranadb/tidb/planner/util"
	"github.com/squareup/pranadb/tidb/sessionctx"
	"github.com/squareup/pranadb/tidb/types"
)

type visitInfo struct {
	privilege        mysql.PrivilegeType
	db               string
	table            string
	column           string
	err              error
	alterWritable    bool
	dynamicPriv      string
	dynamicWithGrant bool
}

// clauseCode indicates in which clause the column is currently.
type clauseCode int

const (
	unknowClause clauseCode = iota
	fieldList
	havingClause
	onClause
	orderByClause
	whereClause
	groupByClause
	globalOrderByClause
	expressionClause
)

var clauseMsg = map[clauseCode]string{
	unknowClause:        "",
	fieldList:           "field list",
	havingClause:        "having clause",
	onClause:            "on clause",
	orderByClause:       "order clause",
	whereClause:         "where clause",
	groupByClause:       "group statement",
	globalOrderByClause: "global ORDER clause",
	expressionClause:    "expression",
}

// PlanBuilder builds Plan from an ast.Node.
// It just builds the ast node straightforwardly.
type PlanBuilder struct {
	ctx          sessionctx.Context
	is           infoschema.InfoSchema
	outerSchemas []*expression.Schema
	outerNames   [][]*types.FieldName
	// colMapper stores the column that must be pre-resolved.
	colMapper map[*ast.ColumnNameExpr]int
	// visitInfo is used for privilege check.
	visitInfo []visitInfo
	// optFlag indicates the flags of the optimizer rules.
	optFlag uint64

	curClause clauseCode

	// rewriterPool stores the expressionRewriter we have created to reuse it if it has been released.
	// rewriterCounter counts how many rewriter is being used.
	rewriterPool    []*expressionRewriter
	rewriterCounter int

	// inStraightJoin represents whether the current "SELECT" statement has
	// "STRAIGHT_JOIN" option.
	inStraightJoin bool

	// handleHelper records the handle column position for tables. Delete/Update/SelectLock/UnionScan may need this information.
	// It collects the information by the following procedure:
	//   Since we build the plan tree from bottom to top, we maintain a stack to record the current handle information.
	//   If it's a dataSource/tableDual node, we create a new map.
	//   If it's a aggregation, we pop the map and push a nil map since no handle information left.
	//   If it's a union, we pop all children's and push a nil map.
	//   If it's a join, we pop its children's out then merge them and push the new map to stack.
	//   If we meet a subquery, it's clearly that it's a independent problem so we just pop one map out when we finish building the subquery.
	handleHelper *handleColHelper

	// selectOffset is the offsets of current processing select stmts.
	selectOffset []int

	// evalDefaultExpr needs this information to find the corresponding column.
	// It stores the OutputNames before buildProjection.
	allNames [][]*types.FieldName

	// correlatedAggMapper stores columns for correlated aggregates which should be evaluated in outer query.
	correlatedAggMapper map[*ast.AggregateFuncExpr]*expression.CorrelatedColumn
}

type handleColHelper struct {
	id2HandleMapStack []map[int64][]HandleCols
	stackTail         int
}

func (hch *handleColHelper) popMap() map[int64][]HandleCols {
	ret := hch.id2HandleMapStack[hch.stackTail-1]
	hch.stackTail--
	hch.id2HandleMapStack = hch.id2HandleMapStack[:hch.stackTail]
	return ret
}

func (hch *handleColHelper) pushMap(m map[int64][]HandleCols) {
	hch.id2HandleMapStack = append(hch.id2HandleMapStack, m)
	hch.stackTail++
}

func (hch *handleColHelper) mergeAndPush(m1, m2 map[int64][]HandleCols) {
	newMap := make(map[int64][]HandleCols, mathutil.Max(len(m1), len(m2)))
	for k, v := range m1 {
		newMap[k] = make([]HandleCols, len(v))
		copy(newMap[k], v)
	}
	for k, v := range m2 {
		if _, ok := newMap[k]; ok {
			newMap[k] = append(newMap[k], v...)
		} else {
			newMap[k] = make([]HandleCols, len(v))
			copy(newMap[k], v)
		}
	}
	hch.pushMap(newMap)
}

func (hch *handleColHelper) tailMap() map[int64][]HandleCols {
	return hch.id2HandleMapStack[hch.stackTail-1]
}

// GetOptFlag gets the optFlag of the PlanBuilder.
func (b *PlanBuilder) GetOptFlag() uint64 {
	return b.optFlag
}

func (b *PlanBuilder) getSelectOffset() int {
	if len(b.selectOffset) > 0 {
		return b.selectOffset[len(b.selectOffset)-1]
	}
	return -1
}

func (b *PlanBuilder) pushSelectOffset(offset int) {
	b.selectOffset = append(b.selectOffset, offset)
}

func (b *PlanBuilder) popSelectOffset() {
	b.selectOffset = b.selectOffset[:len(b.selectOffset)-1]
}

// NewPlanBuilder creates a new PlanBuilder. Return the original PlannerSelectBlockAsName as well, callers decide if
// PlannerSelectBlockAsName should be restored after using this builder.
func NewPlanBuilder(sctx sessionctx.Context, is infoschema.InfoSchema) *PlanBuilder {
	return &PlanBuilder{
		ctx:                 sctx,
		is:                  is,
		colMapper:           make(map[*ast.ColumnNameExpr]int),
		handleHelper:        &handleColHelper{id2HandleMapStack: make([]map[int64][]HandleCols, 0)},
		correlatedAggMapper: make(map[*ast.AggregateFuncExpr]*expression.CorrelatedColumn),
	}
}

// Build builds the ast node to a Plan.
func (b *PlanBuilder) Build(ctx context.Context, node ast.Node) (Plan, error) {
	b.optFlag |= flagPrunColumns
	switch x := node.(type) {
	case *ast.SelectStmt:
		return b.buildSelect(ctx, x)
	case *ast.SetOprStmt:
		return b.buildSetOpr(ctx, x)
	}
	return nil, tidb.ErrUnsupportedType.GenWithStack("Unsupported type %T", node)
}

// detectSelectAgg detects an aggregate function or GROUP BY clause.
func (b *PlanBuilder) detectSelectAgg(sel *ast.SelectStmt) bool {
	if sel.GroupBy != nil {
		return true
	}
	for _, f := range sel.Fields.Fields {
		if f.WildCard != nil {
			continue
		}
		if ast.HasAggFlag(f.Expr) {
			return true
		}
	}
	if sel.Having != nil {
		if ast.HasAggFlag(sel.Having.Expr) {
			return true
		}
	}
	if sel.OrderBy != nil {
		for _, item := range sel.OrderBy.Items {
			if ast.HasAggFlag(item.Expr) {
				return true
			}
		}
	}
	return false
}

func (b *PlanBuilder) detectSelectWindow(sel *ast.SelectStmt) bool {
	for _, f := range sel.Fields.Fields {
		if ast.HasWindowFlag(f.Expr) {
			return true
		}
	}
	if sel.OrderBy != nil {
		for _, item := range sel.OrderBy.Items {
			if ast.HasWindowFlag(item.Expr) {
				return true
			}
		}
	}
	return false
}

func fillContentForTablePath(tablePath *util.AccessPath, tblInfo *model.TableInfo) {
	if tblInfo.IsCommonHandle {
		tablePath.IsCommonHandlePath = true
		for _, index := range tblInfo.Indices {
			if index.Primary {
				tablePath.Index = index
				break
			}
		}
	} else {
		tablePath.IsIntHandlePath = true
	}
}

// getLatestIndexInfo gets the index info of latest schema version from given table id,
// it returns nil if the schema version is not changed
func getLatestIndexInfo(ctx sessionctx.Context, id int64, startVer int64) (map[int64]*model.IndexInfo, bool, error) {
	ismv := ctx.GetInfoSchema()
	is, ok := ismv.(infoschema.InfoSchema)
	if !ok {
		return nil, false, errors.Error("not a infoschema.InfoSchema")
	}
	if is.SchemaMetaVersion() == startVer {
		return nil, false, nil
	}
	latestIndexes := make(map[int64]*model.IndexInfo)
	latestTbl, exist := is.TableByID(id)
	if exist {
		for _, index := range latestTbl.Indices {
			latestIndexes[index.ID] = index
		}
	}
	return latestIndexes, true, nil
}

func getPossibleAccessPaths(ctx sessionctx.Context, tblInfo *model.TableInfo, dbName, tblName model.CIStr, check bool, startVer int64) ([]*util.AccessPath, error) {
	publicPaths := make([]*util.AccessPath, 0, len(tblInfo.Indices)+2)

	tablePath := &util.AccessPath{}
	fillContentForTablePath(tablePath, tblInfo)
	publicPaths = append(publicPaths, tablePath)

	var latestIndexes map[int64]*model.IndexInfo
	var err error

	for _, index := range tblInfo.Indices {
		if index.State == model.StatePublic {
			// Filter out invisible index, because they are not visible for optimizer
			if index.Invisible {
				continue
			}
			if tblInfo.IsCommonHandle && index.Primary {
				continue
			}
			if check && latestIndexes == nil {
				latestIndexes, check, err = getLatestIndexInfo(ctx, tblInfo.ID, 0)
				if err != nil {
					return nil, err
				}
			}
			if check {
				if latestIndex, ok := latestIndexes[index.ID]; !ok || latestIndex.State != model.StatePublic {
					continue
				}
			}
			publicPaths = append(publicPaths, &util.AccessPath{Index: index})
		}
	}

	if len(publicPaths) == 0 {
		publicPaths = append(publicPaths, tablePath)
	}
	return publicPaths, nil
}

// splitWhere split a where expression to a list of AND conditions.
func splitWhere(where ast.ExprNode) []ast.ExprNode {
	var conditions []ast.ExprNode
	switch x := where.(type) {
	case nil:
	case *ast.BinaryOperationExpr:
		if x.Op == opcode.LogicAnd {
			conditions = append(conditions, splitWhere(x.L)...)
			conditions = append(conditions, splitWhere(x.R)...)
		} else {
			conditions = append(conditions, x)
		}
	case *ast.ParenthesesExpr:
		conditions = append(conditions, splitWhere(x.Expr)...)
	default:
		conditions = append(conditions, where)
	}
	return conditions
}
