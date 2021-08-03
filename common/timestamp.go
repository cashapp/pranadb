package common

import (
	"time"

	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
)

type Timestamp = types.Time

// NewTimestampFromStringForTest parses a Timestamp from a string in MySQL datetime format.
func NewTimestampFromStringForTest(str string) Timestamp {
	ts, err := types.ParseTimestamp(&stmtctx.StatementContext{
		TimeZone: time.UTC,
	}, str)
	if err != nil {
		panic(err)
	}
	return ts
}
