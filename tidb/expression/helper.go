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

package expression

import (
	"time"

	"github.com/squareup/pranadb/tidb/sessionctx"
)

func boolToInt64(v bool) int64 {
	if v {
		return 1
	}
	return 0
}

// if timestamp session variable set, use session variable as current time, otherwise use cached time
// during one sql statement, the "current_time" should be the same
func getStmtTimestamp(ctx sessionctx.Context) (time.Time, error) {
	now := time.Now()
	if ctx == nil {
		return now, nil
	}
	return ctx.GetSessionVars().StmtCtx.GetNow(), nil
}
