//
// This source code is a modified form of original source from the TiDB project, which has the following copyright header(s):
//

// Copyright 2017 PingCAP, Inc.
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
	"github.com/pingcap/parser/ast"
	"github.com/squareup/pranadb/tidb/expression"
	"github.com/squareup/pranadb/tidb/types"
)

// AggregateFuncExtractor visits Expr tree.
// It collects AggregateFuncExpr from AST Node.
type AggregateFuncExtractor struct {
	// skipAggMap stores correlated aggregate functions which have been built in outer query,
	// so extractor in sub-query will skip these aggregate functions.
	skipAggMap map[*ast.AggregateFuncExpr]*expression.CorrelatedColumn
	// AggFuncs is the collected AggregateFuncExprs.
	AggFuncs []*ast.AggregateFuncExpr
}

// Enter implements Visitor interface.
func (a *AggregateFuncExtractor) Enter(n ast.Node) (ast.Node, bool) {
	switch n.(type) {
	case *ast.SelectStmt, *ast.SetOprStmt:
		return n, true
	}
	return n, false
}

// Leave implements Visitor interface.
func (a *AggregateFuncExtractor) Leave(n ast.Node) (ast.Node, bool) {
	switch v := n.(type) {
	case *ast.AggregateFuncExpr:
		if _, ok := a.skipAggMap[v]; !ok {
			a.AggFuncs = append(a.AggFuncs, v)
		}
	}
	return n, true
}

// logicalSchemaProducer stores the schema for the logical plans who can produce schema directly.
type logicalSchemaProducer struct {
	schema *expression.Schema
	names  types.NameSlice
	baseLogicalPlan
}

// Schema implements the Plan.Schema interface.
func (s *logicalSchemaProducer) Schema() *expression.Schema {
	if s.schema == nil {
		if len(s.Children()) == 1 {
			// default implementation for plans has only one child: proprgate child schema.
			// multi-children plans are likely to have particular implementation.
			s.schema = s.Children()[0].Schema().Clone()
		} else {
			s.schema = expression.NewSchema()
		}
	}
	return s.schema
}

func (s *logicalSchemaProducer) OutputNames() types.NameSlice {
	if s.names == nil && len(s.Children()) == 1 {
		// default implementation for plans has only one child: proprgate child `OutputNames`.
		// multi-children plans are likely to have particular implementation.
		s.names = s.Children()[0].OutputNames()
	}
	return s.names
}

func (s *logicalSchemaProducer) SetOutputNames(names types.NameSlice) {
	s.names = names
}

// SetSchema implements the Plan.SetSchema interface.
func (s *logicalSchemaProducer) SetSchema(schema *expression.Schema) {
	s.schema = schema
}

func (s *logicalSchemaProducer) setSchemaAndNames(schema *expression.Schema, names types.NameSlice) {
	s.schema = schema
	s.names = names
}

// inlineProjection prunes unneeded columns inline a executor.
func (s *logicalSchemaProducer) inlineProjection(parentUsedCols []*expression.Column) {
	used := expression.GetUsedList(parentUsedCols, s.Schema())
	for i := len(used) - 1; i >= 0; i-- {
		if !used[i] {
			s.schema.Columns = append(s.Schema().Columns[:i], s.Schema().Columns[i+1:]...)
		}
	}
}

// physicalSchemaProducer stores the schema for the physical plans who can produce schema directly.
type physicalSchemaProducer struct {
	schema *expression.Schema
	basePhysicalPlan
}

// Schema implements the Plan.Schema interface.
func (s *physicalSchemaProducer) Schema() *expression.Schema {
	if s.schema == nil {
		if len(s.Children()) == 1 {
			// default implementation for plans has only one child: proprgate child schema.
			// multi-children plans are likely to have particular implementation.
			s.schema = s.Children()[0].Schema().Clone()
		} else {
			s.schema = expression.NewSchema()
		}
	}
	return s.schema
}

// SetSchema implements the Plan.SetSchema interface.
func (s *physicalSchemaProducer) SetSchema(schema *expression.Schema) {
	s.schema = schema
}

func buildLogicalJoinSchema(joinType JoinType, join LogicalPlan) *expression.Schema {
	leftSchema := join.Children()[0].Schema()
	switch joinType {
	case SemiJoin, AntiSemiJoin:
		return leftSchema.Clone()
	case LeftOuterSemiJoin, AntiLeftOuterSemiJoin:
		newSchema := leftSchema.Clone()
		newSchema.Append(join.Schema().Columns[join.Schema().Len()-1])
		return newSchema
	}
	newSchema := expression.MergeSchema(leftSchema, join.Children()[1].Schema())
	if joinType == LeftOuterJoin {
		resetNotNullFlag(newSchema, leftSchema.Len(), newSchema.Len())
	} else if joinType == RightOuterJoin {
		resetNotNullFlag(newSchema, 0, leftSchema.Len())
	}
	return newSchema
}
