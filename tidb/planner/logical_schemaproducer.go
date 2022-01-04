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
	"github.com/squareup/pranadb/tidb/expression"
	"github.com/squareup/pranadb/tidb/types"
)

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
