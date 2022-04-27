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

	GetInfoSchema() InfoschemaMetaVersion

	GetSessionVars() *variable.SessionVars
}
