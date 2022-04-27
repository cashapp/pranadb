//
// This source code is a modified form of original source from the TiDB project, which has the following copyright header(s):
//

// Copyright 2020 PingCAP, Inc.
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
	"strings"

	"github.com/pingcap/parser/model"
	"github.com/squareup/pranadb/tidb/expression"
	"github.com/squareup/pranadb/tidb/sessionctx/stmtctx"
)

// HandleCols is the interface that holds handle columns.
type HandleCols interface {
	// ResolveIndices resolves handle column indices.
	ResolveIndices(schema *expression.Schema) (HandleCols, error)
	// IsInt returns if the HandleCols is a single tnt column.
	IsInt() bool
	// String implements the fmt.Stringer interface.
	String() string
	// GetCol gets the column by idx.
	GetCol(idx int) *expression.Column
	// NumCols returns the number of columns.
	NumCols() int
}

// CommonHandleCols implements the kv.HandleCols interface.
type CommonHandleCols struct {
	tblInfo *model.TableInfo
	idxInfo *model.IndexInfo
	columns []*expression.Column
	sc      *stmtctx.StatementContext
}

// ResolveIndices implements the kv.HandleCols interface.
func (cb *CommonHandleCols) ResolveIndices(schema *expression.Schema) (HandleCols, error) {
	ncb := &CommonHandleCols{
		tblInfo: cb.tblInfo,
		idxInfo: cb.idxInfo,
		sc:      cb.sc,
		columns: make([]*expression.Column, len(cb.columns)),
	}
	for i, col := range cb.columns {
		newCol, err := col.ResolveIndices(schema)
		if err != nil {
			return nil, err
		}
		ncb.columns[i] = newCol.(*expression.Column)
	}
	return ncb, nil
}

// IsInt implements the kv.HandleCols interface.
func (cb *CommonHandleCols) IsInt() bool {
	return false
}

// GetCol implements the kv.HandleCols interface.
func (cb *CommonHandleCols) GetCol(idx int) *expression.Column {
	return cb.columns[idx]
}

// NumCols implements the kv.HandleCols interface.
func (cb *CommonHandleCols) NumCols() int {
	return len(cb.columns)
}

// String implements the kv.HandleCols interface.
func (cb *CommonHandleCols) String() string {
	b := new(strings.Builder)
	b.WriteByte('[')
	for i, col := range cb.columns {
		if i != 0 {
			b.WriteByte(',')
		}
		b.WriteString(col.OrigName)
	}
	b.WriteByte(']')
	return b.String()
}

// NewCommonHandleCols creates a new CommonHandleCols.
func NewCommonHandleCols(sc *stmtctx.StatementContext, tblInfo *model.TableInfo, idxInfo *model.IndexInfo,
	tableColumns []*expression.Column) *CommonHandleCols {
	cols := &CommonHandleCols{
		tblInfo: tblInfo,
		idxInfo: idxInfo,
		sc:      sc,
		columns: make([]*expression.Column, len(idxInfo.Columns)),
	}
	for i, idxCol := range idxInfo.Columns {
		cols.columns[i] = tableColumns[idxCol.Offset]
	}
	return cols
}

// IntHandleCols implements the kv.HandleCols interface.
type IntHandleCols struct {
	col *expression.Column
}

// ResolveIndices implements the kv.HandleCols interface.
func (ib *IntHandleCols) ResolveIndices(schema *expression.Schema) (HandleCols, error) {
	newCol, err := ib.col.ResolveIndices(schema)
	if err != nil {
		return nil, err
	}
	return &IntHandleCols{col: newCol.(*expression.Column)}, nil
}

// IsInt implements the kv.HandleCols interface.
func (ib *IntHandleCols) IsInt() bool {
	return true
}

// String implements the kv.HandleCols interface.
func (ib *IntHandleCols) String() string {
	return ib.col.OrigName
}

// GetCol implements the kv.HandleCols interface.
func (ib *IntHandleCols) GetCol(idx int) *expression.Column {
	if idx != 0 {
		return nil
	}
	return ib.col
}

// NumCols implements the kv.HandleCols interface.
func (ib *IntHandleCols) NumCols() int {
	return 1
}
