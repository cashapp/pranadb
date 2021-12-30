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

package expression

import (
	"github.com/pingcap/parser/ast"
	"github.com/squareup/pranadb/tidb"
	"github.com/squareup/pranadb/tidb/types"
)

// FindFieldName finds the column name from NameSlice.
func FindFieldName(names types.NameSlice, astCol *ast.ColumnName) (int, error) {
	dbName, tblName, colName := astCol.Schema, astCol.Table, astCol.Name
	idx := -1
	for i, name := range names {
		if !name.NotExplicitUsable && (dbName.L == "" || dbName.L == name.DBName.L) &&
			(tblName.L == "" || tblName.L == name.TblName.L) &&
			(colName.L == name.ColName.L) {
			if idx == -1 {
				idx = i
			} else {
				return -1, tidb.ErrNonUniq.GenWithStackByArgs(astCol.String(), "field list")
			}
		}
	}
	return idx, nil
}
