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

package infoschema

import (
	"github.com/pingcap/parser/model"
	"github.com/squareup/pranadb/tidb/table"
)

type InfoSchema interface {
	SchemaByName(schema model.CIStr) (*model.DBInfo, bool)
	TableByName(schema, table model.CIStr) (table.Table, error)
	TableByID(id int64) (table.Table, bool)
	SchemaMetaVersion() int64
}
