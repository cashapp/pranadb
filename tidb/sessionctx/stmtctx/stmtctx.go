/*
 *  Copyright 2022 Square Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 * This source code is a based on original source from the TiDB project
 */

package stmtctx

import (
	"math"
	"sync"
	"time"

	"github.com/pingcap/parser/model"
)

const (
	// WarnLevelError represents level "Error" for 'SHOW WARNINGS' syntax.
	WarnLevelError = "Error"
	// WarnLevelWarning represents level "Warning" for 'SHOW WARNINGS' syntax.
	WarnLevelWarning = "Warning"
)

// SQLWarn relates a sql warning and it's level.
type SQLWarn struct {
	Level string
	Err   error
}

// StatementContext contains variables for a statement.
// It should be reset before executing a statement.
type StatementContext struct {

	// InReorgAttribute is indicated for cast function that the transition is a kind of reorg process.
	InReorgAttribute bool

	IgnoreTruncate            bool
	UseCache                  bool
	InNullRejectCheck         bool
	OptimDependOnMutableConst bool

	// mu struct holds variables that change during execution.
	mu struct {
		sync.Mutex
		warnings   []SQLWarn
		errorCount uint16
	}

	// Copied from SessionVars.TimeZone.
	TimeZone *time.Location

	TblInfo2UnionScan map[*model.TableInfo]bool

	now *time.Time
}

func (sc *StatementContext) GetNow() time.Time {
	if sc.now == nil {
		now := time.Now()
		sc.now = &now
	}
	return *sc.now
}

// TableEntry presents table in db.
type TableEntry struct {
	DB    string
	Table string
}

// TruncateWarnings truncates warnings begin from start and returns the truncated warnings.
func (sc *StatementContext) TruncateWarnings(start int) []SQLWarn {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sz := len(sc.mu.warnings) - start
	if sz <= 0 {
		return nil
	}
	ret := make([]SQLWarn, sz)
	copy(ret, sc.mu.warnings[start:])
	sc.mu.warnings = sc.mu.warnings[:start]
	return ret
}

// WarningCount gets warning count.
func (sc *StatementContext) WarningCount() uint16 {
	sc.mu.Lock()
	wc := uint16(len(sc.mu.warnings))
	sc.mu.Unlock()
	return wc
}

// AppendWarning appends a warning with level 'Warning'.
func (sc *StatementContext) AppendWarning(warn error) {
	sc.mu.Lock()
	if len(sc.mu.warnings) < math.MaxUint16 {
		sc.mu.warnings = append(sc.mu.warnings, SQLWarn{WarnLevelWarning, warn})
	}
	sc.mu.Unlock()
}

// AppendError appends a warning with level 'Error'.
func (sc *StatementContext) AppendError(warn error) {
	sc.mu.Lock()
	if len(sc.mu.warnings) < math.MaxUint16 {
		sc.mu.warnings = append(sc.mu.warnings, SQLWarn{WarnLevelError, warn})
		sc.mu.errorCount++
	}
	sc.mu.Unlock()
}

// HandleTruncate ignores or returns the error based on the StatementContext state.
func (sc *StatementContext) HandleTruncate(err error) error {
	// TODO: At present we have not checked whether the error can be ignored or treated as warning.
	// We will do that later, and then append WarnDataTruncated instead of the error itself.
	if err == nil {
		return nil
	}
	if sc.IgnoreTruncate {
		return nil
	}
	return err
}

// HandleOverflow treats ErrOverflow as warnings or returns the error based on the StmtCtx.OverflowAsWarning state.
func (sc *StatementContext) HandleOverflow(err error, warnErr error) error {
	if err == nil {
		return nil
	}
	return err
}

// ShouldClipToZero indicates whether values less than 0 should be clipped to 0 for unsigned integer types.
// This is the case for `insert`, `update`, `alter table`, `create table` and `load data infile` statements, when not in strict SQL mode.
// see https://dev.mysql.com/doc/refman/5.7/en/out-of-range-and-overflow.html
func (sc *StatementContext) ShouldClipToZero() bool {
	return false
}

// ShouldIgnoreOverflowError indicates whether we should ignore the error when type conversion overflows,
// so we can leave it for further processing like clipping values less than 0 to 0 for unsigned integer types.
func (sc *StatementContext) ShouldIgnoreOverflowError() bool {
	return false
}
