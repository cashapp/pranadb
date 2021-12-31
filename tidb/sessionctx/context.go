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

package sessionctx

import (
	"context"
	"fmt"
	"github.com/squareup/pranadb/tidb/sessionctx/variable"
)

// InfoschemaMetaVersion is a workaround. Due to circular dependency,
// can not return the complete interface. But SchemaMetaVersion is widely used for logging.
// So we give a convenience for that.
// FIXME: remove this interface
type InfoschemaMetaVersion interface {
	SchemaMetaVersion() int64
}

// Context is an interface for transaction and executive args environment.
type Context interface {

	// SetValue saves a value associated with this context for key.
	SetValue(key fmt.Stringer, value interface{})

	// Value returns the value associated with this context for key.
	Value(key fmt.Stringer) interface{}

	// ClearValue clears the value associated with this context for key.
	ClearValue(key fmt.Stringer)

	GetInfoSchema() InfoschemaMetaVersion

	GetSessionVars() *variable.SessionVars
}

type basicCtxType int

func (t basicCtxType) String() string {
	switch t {
	case QueryString:
		return "query_string"
	case Initing:
		return "initing"
	case LastExecuteDDL:
		return "last_execute_ddl"
	}
	return "unknown"
}

// Context keys.
const (
	// QueryString is the key for original query string.
	QueryString basicCtxType = 1
	// Initing is the key for indicating if the server is running bootstrap or upgrade job.
	Initing basicCtxType = 2
	// LastExecuteDDL is the key for whether the session execute a ddl command last time.
	LastExecuteDDL basicCtxType = 3
)

// ValidateStaleReadTS validates that readTS does not exceed the current time not strictly.
func ValidateStaleReadTS(ctx context.Context, sctx Context, readTS uint64) error {
	return nil
}
