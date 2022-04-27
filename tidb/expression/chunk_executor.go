//
// This source code is a modified form of original source from the TiDB project, which has the following copyright header(s):
//

// Copyright 2017 PingCAP, Inc.
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
	"github.com/pingcap/parser/ast"
)

// HasGetSetVarFunc checks whether an expression contains SetVar/GetVar function.
func HasGetSetVarFunc(expr Expression) bool {
	scalaFunc, ok := expr.(*ScalarFunction)
	if !ok {
		return false
	}
	if scalaFunc.FuncName.L == ast.SetVar {
		return true
	}
	if scalaFunc.FuncName.L == ast.GetVar {
		return true
	}
	for _, arg := range scalaFunc.GetArgs() {
		if HasGetSetVarFunc(arg) {
			return true
		}
	}
	return false
}

// HasAssignSetVarFunc checks whether an expression contains SetVar function and assign a value
func HasAssignSetVarFunc(expr Expression) bool {
	scalaFunc, ok := expr.(*ScalarFunction)
	if !ok {
		return false
	}
	if scalaFunc.FuncName.L == ast.SetVar {
		for _, arg := range scalaFunc.GetArgs() {
			if _, ok := arg.(*ScalarFunction); ok {
				return true
			}
		}
	}
	for _, arg := range scalaFunc.GetArgs() {
		if HasAssignSetVarFunc(arg) {
			return true
		}
	}
	return false
}
