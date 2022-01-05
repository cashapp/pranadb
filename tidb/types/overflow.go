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

package types

import (
	"fmt"
	"github.com/squareup/pranadb/tidb"
	"math"
	"time"
)

// AddInt64 adds int64 a and b if no overflow, otherwise returns error.
func AddInt64(a int64, b int64) (int64, error) {
	if (a > 0 && b > 0 && math.MaxInt64-a < b) ||
		(a < 0 && b < 0 && math.MinInt64-a > b) {
		return 0, tidb.ErrOverflow.GenWithStackByArgs("BIGINT", fmt.Sprintf("(%d, %d)", a, b))
	}

	return a + b, nil
}

// AddDuration adds time.Duration a and b if no overflow, otherwise returns error.
func AddDuration(a time.Duration, b time.Duration) (time.Duration, error) {
	if (a > 0 && b > 0 && math.MaxInt64-a < b) ||
		(a < 0 && b < 0 && math.MinInt64-a > b) {
		return 0, tidb.ErrOverflow.GenWithStackByArgs("BIGINT", fmt.Sprintf("(%d, %d)", int64(a), int64(b)))
	}

	return a + b, nil
}

// SubDuration subtracts time.Duration a with b and returns time.Duration if no overflow error.
func SubDuration(a time.Duration, b time.Duration) (time.Duration, error) {
	if (a > 0 && b < 0 && math.MaxInt64-a < -b) ||
		(a < 0 && b > 0 && math.MinInt64-a > -b) ||
		(a == 0 && b == math.MinInt64) {
		return 0, tidb.ErrOverflow.GenWithStackByArgs("BIGINT", fmt.Sprintf("(%d, %d)", a, b))
	}
	return a - b, nil
}

// SubInt64 subtracts int64 a with b and returns int64 if no overflow error.
func SubInt64(a int64, b int64) (int64, error) {
	if (a > 0 && b < 0 && math.MaxInt64-a < -b) ||
		(a < 0 && b > 0 && math.MinInt64-a > -b) ||
		(a == 0 && b == math.MinInt64) {
		return 0, tidb.ErrOverflow.GenWithStackByArgs("BIGINT", fmt.Sprintf("(%d, %d)", a, b))
	}
	return a - b, nil
}

// DivInt64 divides int64 a with b, returns int64 if no overflow error.
// It just checks overflow, if b is zero, a "divide by zero" panic throws.
func DivInt64(a int64, b int64) (int64, error) {
	if a == math.MinInt64 && b == -1 {
		return 0, tidb.ErrOverflow.GenWithStackByArgs("BIGINT", fmt.Sprintf("(%d, %d)", a, b))
	}

	return a / b, nil
}

// DivUintWithInt divides uint64 a with int64 b, returns uint64 if no overflow error.
// It just checks overflow, if b is zero, a "divide by zero" panic throws.
func DivUintWithInt(a uint64, b int64) (uint64, error) {
	if b < 0 {
		if a != 0 && uint64(-b) <= a {
			return 0, tidb.ErrOverflow.GenWithStackByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%d, %d)", a, b))
		}

		return 0, nil
	}

	return a / uint64(b), nil
}

// DivIntWithUint divides int64 a with uint64 b, returns uint64 if no overflow error.
// It just checks overflow, if b is zero, a "divide by zero" panic throws.
func DivIntWithUint(a int64, b uint64) (uint64, error) {
	if a < 0 {
		if uint64(-a) >= b {
			return 0, tidb.ErrOverflow.GenWithStackByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%d, %d)", a, b))
		}

		return 0, nil
	}

	return uint64(a) / b, nil
}
