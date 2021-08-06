package common

import (
	"time"

	"github.com/pingcap/parser/mysql"

	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
)

type Timestamp = types.Time

// NewTimestampFromString parses a Timestamp from a string in MySQL datetime format.
func NewTimestampFromString(str string) Timestamp {
	ts, err := types.ParseTimestamp(&stmtctx.StatementContext{
		TimeZone: time.UTC,
	}, str)
	if err != nil {
		panic(err)
	}
	return ts
}

func NewTimestampFromGoTime(t time.Time) Timestamp {
	return types.NewTime(types.FromGoTime(t.UTC()), mysql.TypeTimestamp, 6)
}

func NewTimestampFromUnixEpochMillis(v int64) Timestamp {
	sec := v / 1000
	nsec := (v % 1000) * 1000000
	gotime := time.Unix(sec, nsec)
	return NewTimestampFromGoTime(gotime)
}
