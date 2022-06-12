package common

import (
	"math"
	"time"

	"github.com/pingcap/parser/mysql"

	"github.com/squareup/pranadb/tidb/sessionctx/stmtctx"
	"github.com/squareup/pranadb/tidb/types"
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

var fspDurations [7]time.Duration

func init() {
	for i := 0; i < 7; i++ {
		fspDurations[i] = time.Duration(math.Pow10(9 - i))
	}
}

func RoundTimestampToFSP(ts *Timestamp, fsp int8) error {
	gt, err := ts.GoTime(time.UTC)
	if err != nil {
		return err
	}
	dur := fspDurations[fsp]
	gt = gt.Round(dur)
	ct := types.FromGoTime(gt)
	ts.SetCoreTime(ct)
	return nil
}
