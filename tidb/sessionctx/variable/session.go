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

package variable

import (
	"time"

	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/squareup/pranadb/tidb/sessionctx/stmtctx"
	"github.com/squareup/pranadb/tidb/types"
	"github.com/squareup/pranadb/tidb/util/timeutil"
)

const (
	DefOptCPUFactor                  = 3.0
	DefOptNetworkFactor              = 1.0
	DefOptScanFactor                 = 1.5
	DefOptDescScanFactor             = 3.0
	DefOptSeekFactor                 = 20.0
	DefOptMemoryFactor               = 0.001
	DefOptDiskFactor                 = 1.5
	DefOptConcurrencyFactor          = 3.0
	DefTiDBProjectionConcurrency     = ConcurrencyUnset
	DefTiDBHashAggPartialConcurrency = ConcurrencyUnset
	DefTiDBHashAggFinalConcurrency   = ConcurrencyUnset
	DefEnableVectorizedExpression    = true
	DefExecutorConcurrency           = 5
	DefOptimizerSelectivityLevel     = 0
)

// SessionVars is to handle user-defined or global variables in the current session.
type SessionVars struct {
	Concurrency

	// PreparedParams params for prepared statements
	PreparedParams PreparedParams

	// Status stands for the session status. e.g. in transaction or not, auto commit is on or off, and so on.
	Status uint16

	// PlanID is the unique id of logical and physical plan.
	PlanID int

	// PlanColumnID is the unique id for column when building plan.
	PlanColumnID int64

	// CurrentDB is the default database of this session.
	CurrentDB string

	// StmtCtx holds variables for current executing statement.
	StmtCtx *stmtctx.StatementContext

	// CPUFactor is the CPU cost of processing one expression for one row.
	CPUFactor float64
	// networkFactor is the network cost of transferring 1 byte data.
	networkFactor float64
	// ScanFactor is the IO cost of scanning 1 byte data on TiKV and TiFlash.
	scanFactor float64
	// descScanFactor is the IO cost of scanning 1 byte data on TiKV and TiFlash in desc order.
	descScanFactor float64
	// seekFactor is the IO cost of seeking the start value of a range in TiKV or TiFlash.
	seekFactor float64
	// MemoryFactor is the memory cost of storing one tuple.
	MemoryFactor float64
	// DiskFactor is the IO cost of reading/writing one byte to temporary disk.
	DiskFactor float64
	// ConcurrencyFactor is the CPU cost of additional one goroutine.
	ConcurrencyFactor float64

	// Per-connection time zones. Each client that connects has its own time zone setting, given by the session time_zone variable.
	// See https://dev.mysql.com/doc/refman/5.7/en/time-zone-support.html
	TimeZone *time.Location

	SQLMode mysql.SQLMode

	// EnableVectorizedExpression  enables the vectorized expression evaluation.
	EnableVectorizedExpression bool

	MaxAllowedPacket  uint64
	DefaultWeekFormat string

	// OptimizerSelectivityLevel defines the level of the selectivity estimation in plan.
	OptimizerSelectivityLevel int
}

// PreparedParams contains the parameters of the current prepared statement when executing it.
type PreparedParams []types.Datum

func (pps PreparedParams) String() string {
	if len(pps) == 0 {
		return ""
	}
	return " [arguments: " + types.DatumsToStrNoErr(pps) + "]"
}

// NewSessionVars creates a session vars object.
func NewSessionVars() *SessionVars {
	vars := &SessionVars{
		PreparedParams:             make([]types.Datum, 0, 10),
		Status:                     mysql.ServerStatusAutocommit,
		StmtCtx:                    new(stmtctx.StatementContext),
		CPUFactor:                  DefOptCPUFactor,
		networkFactor:              DefOptNetworkFactor,
		scanFactor:                 DefOptScanFactor,
		descScanFactor:             DefOptDescScanFactor,
		seekFactor:                 DefOptSeekFactor,
		MemoryFactor:               DefOptMemoryFactor,
		DiskFactor:                 DefOptDiskFactor,
		ConcurrencyFactor:          DefOptConcurrencyFactor,
		EnableVectorizedExpression: DefEnableVectorizedExpression,
		TimeZone:                   timeutil.SystemLocation(),
		MaxAllowedPacket:           67108864,
		DefaultWeekFormat:          "0",
		OptimizerSelectivityLevel:  DefOptimizerSelectivityLevel,
	}
	vars.Concurrency = Concurrency{
		projectionConcurrency:     DefTiDBProjectionConcurrency,
		hashAggPartialConcurrency: DefTiDBHashAggPartialConcurrency,
		hashAggFinalConcurrency:   DefTiDBHashAggFinalConcurrency,
		ExecutorConcurrency:       DefExecutorConcurrency,
	}
	return vars
}

// AllocPlanColumnID allocates column id for plan.
func (s *SessionVars) AllocPlanColumnID() int64 {
	s.PlanColumnID++
	return s.PlanColumnID
}

// GetCharsetInfo gets charset and collation for current context.
// What character set should the server translate a statement to after receiving it?
// For this, the server uses the character_set_connection and collation_connection system variables.
// It converts statements sent by the client from character_set_client to character_set_connection
// (except for string literals that have an introducer such as _latin1 or _utf8).
// collation_connection is important for comparisons of literal strings.
// For comparisons of strings with column values, collation_connection does not matter because columns
// have their own collation, which has a higher collation precedence.
// See https://dev.mysql.com/doc/refman/5.7/en/charset-connection.html
func (s *SessionVars) GetCharsetInfo() (charset, collation string) {
	charset = "utf8mb4_bin"
	collation = "utf8mb4_bin"
	return
}

// Location returns the value of time_zone session variable. If it is nil, then return time.Local.
func (s *SessionVars) Location() *time.Location {
	loc := s.TimeZone
	if loc == nil {
		loc = timeutil.SystemLocation()
	}
	return loc
}

// ConcurrencyUnset means the value the of the concurrency related variable is unset.
const ConcurrencyUnset = -1

// Concurrency defines concurrency values.
type Concurrency struct {

	// projectionConcurrency is the number of concurrent projection worker.
	// projectionConcurrency is deprecated, use ExecutorConcurrency instead.
	projectionConcurrency int

	// hashAggPartialConcurrency is the number of concurrent hash aggregation partial worker.
	// hashAggPartialConcurrency is deprecated, use ExecutorConcurrency instead.
	hashAggPartialConcurrency int

	// hashAggFinalConcurrency is the number of concurrent hash aggregation final worker.
	// hashAggFinalConcurrency is deprecated, use ExecutorConcurrency instead.
	hashAggFinalConcurrency int

	// ExecutorConcurrency is the number of concurrent worker for all executors.
	ExecutorConcurrency int
}

// ProjectionConcurrency return the number of concurrent projection worker.
func (c *Concurrency) ProjectionConcurrency() int {
	if c.projectionConcurrency != ConcurrencyUnset {
		return c.projectionConcurrency
	}
	return c.ExecutorConcurrency
}

// HashAggPartialConcurrency return the number of concurrent hash aggregation partial worker.
func (c *Concurrency) HashAggPartialConcurrency() int {
	if c.hashAggPartialConcurrency != ConcurrencyUnset {
		return c.hashAggPartialConcurrency
	}
	return c.ExecutorConcurrency
}

// HashAggFinalConcurrency return the number of concurrent hash aggregation final worker.
func (c *Concurrency) HashAggFinalConcurrency() int {
	if c.hashAggFinalConcurrency != ConcurrencyUnset {
		return c.hashAggFinalConcurrency
	}
	return c.ExecutorConcurrency
}

// GetNetworkFactor returns the session variable networkFactor
// returns 0 when tbl is a temporary table.
func (s *SessionVars) GetNetworkFactor(tbl *model.TableInfo) float64 {
	if tbl != nil {
		if tbl.TempTableType != model.TempTableNone {
			return 0
		}
	}
	return s.networkFactor
}

// GetScanFactor returns the session variable scanFactor
// returns 0 when tbl is a temporary table.
func (s *SessionVars) GetScanFactor(tbl *model.TableInfo) float64 {
	if tbl != nil {
		if tbl.TempTableType != model.TempTableNone {
			return 0
		}
	}
	return s.scanFactor
}

// GetDescScanFactor returns the session variable descScanFactor
// returns 0 when tbl is a temporary table.
func (s *SessionVars) GetDescScanFactor(tbl *model.TableInfo) float64 {
	if tbl != nil {
		if tbl.TempTableType != model.TempTableNone {
			return 0
		}
	}
	return s.descScanFactor
}

// GetSeekFactor returns the session variable seekFactor
// returns 0 when tbl is a temporary table.
func (s *SessionVars) GetSeekFactor(tbl *model.TableInfo) float64 {
	if tbl != nil {
		if tbl.TempTableType != model.TempTableNone {
			return 0
		}
	}
	return s.seekFactor
}
