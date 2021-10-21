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

package executor

import (
	"context"

	"github.com/pingcap/tidb/util/chunk"
)

// AdminShowTelemetryExec is an executor for ADMIN SHOW TELEMETRY.
type AdminShowTelemetryExec struct {
	baseExecutor
	done bool
}

// Next implements the Executor Next interface.
func (e *AdminShowTelemetryExec) Next(ctx context.Context, req *chunk.Chunk) error {

	return nil
}

// AdminResetTelemetryIDExec is an executor for ADMIN RESET TELEMETRY_ID.
type AdminResetTelemetryIDExec struct {
	baseExecutor
	done bool
}

// Next implements the Executor Next interface.
func (e *AdminResetTelemetryIDExec) Next(ctx context.Context, _ *chunk.Chunk) error {
	return nil
}
