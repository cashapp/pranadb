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

// Operand is the node of a pattern tree, it represents a logical expression operator.
// Different from logical plan operator which holds the full information about an expression
// operator, Operand only stores the type information.
// An Operand may correspond to a concrete logical plan operator, or it can has special meaning,
// e.g, a placeholder for any logical plan operator.
type Operand int

const (
	// OperandAny is a placeholder for any Operand.
	OperandAny Operand = iota
	// OperandJoin is the operand for LogicalJoin.
	OperandJoin
	// OperandAggregation is the operand for LogicalAggregation.
	OperandAggregation
	// OperandProjection is the operand for LogicalProjection.
	OperandProjection
	// OperandSelection is the operand for LogicalSelection.
	OperandSelection
	// OperandDataSource is the operand for LogicalDataSource.
	OperandDataSource
	// OperandUnionAll is the operand for LogicalUnionAll.
	OperandUnionAll
	// OperandSort is the operand for LogicalSort.
	OperandSort
	// OperandTopN is the operand for LogicalTopN.
	OperandTopN
	// OperandLimit is the operand for LogicalLimit.
	OperandLimit
	// OperandTableScan is the operand for TableScan.
	OperandTableScan
	// OperandIndexScan is the operand for IndexScan.
	OperandIndexScan
	// OperandUnsupported is the operand for unsupported operators.
	OperandUnsupported
)

// GetOperand maps logical plan operator to Operand.
func GetOperand(p LogicalPlan) Operand {
	switch p.(type) {
	case *LogicalJoin:
		return OperandJoin
	case *LogicalAggregation:
		return OperandAggregation
	case *LogicalProjection:
		return OperandProjection
	case *LogicalSelection:
		return OperandSelection
	case *LogicalDataSource:
		return OperandDataSource
	case *LogicalUnionAll:
		return OperandUnionAll
	case *LogicalSort:
		return OperandSort
	case *LogicalTopN:
		return OperandTopN
	case *LogicalLimit:
		return OperandLimit
	case *LogicalTableScan:
		return OperandTableScan
	case *LogicalIndexScan:
		return OperandIndexScan
	default:
		return OperandUnsupported
	}
}

// Match checks if current Operand matches specified one.
func (o Operand) Match(t Operand) bool {
	if o == OperandAny || t == OperandAny {
		return true
	}
	if o == t {
		return true
	}
	return false
}

// Pattern defines the match pattern for a rule. It's a tree-like structure
// which is a piece of a logical expression. Each node in the Pattern tree is
// defined by an Operand and EngineType pair.
type Pattern struct {
	Operand
	Children []*Pattern
}

// Match checks whether the EngineTypeSet contains the given EngineType
// and whether the two Operands match.
func (p *Pattern) Match(o Operand) bool {
	return p.Operand.Match(o)
}

// MatchOperandAny checks whether the pattern's Operand is OperandAny
// and the EngineTypeSet contains the given EngineType.
func (p *Pattern) MatchOperandAny() bool {
	return p.Operand == OperandAny
}

// NewPattern creates a pattern node according to the Operand and EngineType.
func NewPattern(operand Operand) *Pattern {
	return &Pattern{Operand: operand}
}

// SetChildren sets the Children information for a pattern node.
func (p *Pattern) SetChildren(children ...*Pattern) {
	p.Children = children
}

// BuildPattern builds a Pattern from Operand, EngineType and child Patterns.
// Used in GetPattern() of Transformation interface to generate a Pattern.
func BuildPattern(operand Operand, children ...*Pattern) *Pattern {
	p := &Pattern{Operand: operand}
	p.Children = children
	return p
}
