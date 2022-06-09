// Copyright 2016 PingCAP, Inc.
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
	"github.com/squareup/pranadb/tidb/types"
	"github.com/squareup/pranadb/tidb/types/json"
	"github.com/squareup/pranadb/tidb/util/chunk"
	"github.com/squareup/pranadb/tidb/util/collate"
	"github.com/squareup/pranadb/tidb/util/set"
	"time"

	"github.com/pingcap/parser/mysql"
	"github.com/squareup/pranadb/tidb/sessionctx"
)

var (
	_ functionClass = &inFunctionClass{}
)

var (
	_ builtinFunc = &builtinInIntSig{}
	_ builtinFunc = &builtinInStringSig{}
	_ builtinFunc = &builtinInDecimalSig{}
	_ builtinFunc = &builtinInRealSig{}
	_ builtinFunc = &builtinInTimeSig{}
	_ builtinFunc = &builtinInDurationSig{}
	_ builtinFunc = &builtinInJSONSig{}
)

type inFunctionClass struct {
	baseFunctionClass
}

func (c *inFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (sig builtinFunc, err error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTps := make([]types.EvalType, len(args))
	for i := range args {
		argTps[i] = args[0].GetType().EvalType()
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, argTps...)
	if err != nil {
		return nil, err
	}
	for i := 1; i < len(args); i++ {
		DisableParseJSONFlag4Expr(args[i])
	}
	bf.tp.Flen = 1
	switch args[0].GetType().EvalType() {
	case types.ETInt:
		inInt := builtinInIntSig{baseInSig: baseInSig{baseBuiltinFunc: bf}}
		err := inInt.buildHashMapForConstArgs(ctx)
		if err != nil {
			return &inInt, err
		}
		sig = &inInt
	case types.ETString:
		inStr := builtinInStringSig{baseInSig: baseInSig{baseBuiltinFunc: bf}}
		err := inStr.buildHashMapForConstArgs(ctx)
		if err != nil {
			return &inStr, err
		}
		sig = &inStr
	case types.ETReal:
		inReal := builtinInRealSig{baseInSig: baseInSig{baseBuiltinFunc: bf}}
		err := inReal.buildHashMapForConstArgs(ctx)
		if err != nil {
			return &inReal, err
		}
		sig = &inReal
	case types.ETDecimal:
		inDecimal := builtinInDecimalSig{baseInSig: baseInSig{baseBuiltinFunc: bf}}
		err := inDecimal.buildHashMapForConstArgs(ctx)
		if err != nil {
			return &inDecimal, err
		}
		sig = &inDecimal
	case types.ETDatetime, types.ETTimestamp:
		inTime := builtinInTimeSig{baseInSig: baseInSig{baseBuiltinFunc: bf}}
		err := inTime.buildHashMapForConstArgs(ctx)
		if err != nil {
			return &inTime, err
		}
		sig = &inTime
	case types.ETDuration:
		inDuration := builtinInDurationSig{baseInSig: baseInSig{baseBuiltinFunc: bf}}
		err := inDuration.buildHashMapForConstArgs(ctx)
		if err != nil {
			return &inDuration, err
		}
		sig = &inDuration
	case types.ETJson:
		sig = &builtinInJSONSig{baseBuiltinFunc: bf}
	}
	return sig, nil
}

// nolint:structcheck
type baseInSig struct {
	baseBuiltinFunc
	// nonConstArgsIdx stores the indices of non-constant args in the baseBuiltinFunc.args (the first arg is not included).
	// It works with builtinInXXXSig.hashset to accelerate 'eval'.
	nonConstArgsIdx []int
	hasNull         bool
}

// builtinInIntSig see https://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_in
type builtinInIntSig struct {
	baseInSig
	// the bool value in the map is used to identify whether the constant stored in key is signed or unsigned
	hashSet map[int64]bool
}

func (b *builtinInIntSig) buildHashMapForConstArgs(ctx sessionctx.Context) error {
	b.nonConstArgsIdx = make([]int, 0)
	b.hashSet = make(map[int64]bool, len(b.args)-1)
	for i := 1; i < len(b.args); i++ {
		if b.args[i].ConstItem(b.ctx.GetSessionVars().StmtCtx) {
			val, isNull, err := b.args[i].EvalInt(ctx, chunk.Row{})
			if err != nil {
				return err
			}
			if isNull {
				b.hasNull = true
				continue
			}
			b.hashSet[val] = mysql.HasUnsignedFlag(b.args[i].GetType().Flag)
		} else {
			b.nonConstArgsIdx = append(b.nonConstArgsIdx, i)
		}
	}
	return nil
}

func (b *builtinInIntSig) Clone() builtinFunc {
	newSig := &builtinInIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.nonConstArgsIdx = make([]int, len(b.nonConstArgsIdx))
	copy(newSig.nonConstArgsIdx, b.nonConstArgsIdx)
	newSig.hashSet = b.hashSet
	newSig.hasNull = b.hasNull
	return newSig
}

func (b *builtinInIntSig) evalInt(row chunk.Row) (int64, bool, error) {
	arg0, isNull0, err := b.args[0].EvalInt(b.ctx, row)
	if isNull0 || err != nil {
		return 0, isNull0, err
	}
	isUnsigned0 := mysql.HasUnsignedFlag(b.args[0].GetType().Flag)

	args := b.args[1:]
	if len(b.hashSet) != 0 {
		if isUnsigned, ok := b.hashSet[arg0]; ok {
			if (isUnsigned0 && isUnsigned) || (!isUnsigned0 && !isUnsigned) {
				return 1, false, nil
			}
			if arg0 >= 0 {
				return 1, false, nil
			}
		}
		args = make([]Expression, 0, len(b.nonConstArgsIdx))
		for _, i := range b.nonConstArgsIdx {
			args = append(args, b.args[i])
		}
	}

	hasNull := b.hasNull
	for _, arg := range args {
		evaledArg, isNull, err := arg.EvalInt(b.ctx, row)
		if err != nil {
			return 0, true, err
		}
		if isNull {
			hasNull = true
			continue
		}
		isUnsigned := mysql.HasUnsignedFlag(arg.GetType().Flag)
		if isUnsigned0 && isUnsigned {
			if evaledArg == arg0 {
				return 1, false, nil
			}
		} else if !isUnsigned0 && !isUnsigned {
			if evaledArg == arg0 {
				return 1, false, nil
			}
		} else if !isUnsigned0 && isUnsigned {
			if arg0 >= 0 && evaledArg == arg0 {
				return 1, false, nil
			}
		} else {
			if evaledArg >= 0 && evaledArg == arg0 {
				return 1, false, nil
			}
		}
	}
	return 0, hasNull, nil
}

// builtinInStringSig see https://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_in
type builtinInStringSig struct {
	baseInSig
	hashSet set.StringSet
}

func (b *builtinInStringSig) buildHashMapForConstArgs(ctx sessionctx.Context) error {
	b.nonConstArgsIdx = make([]int, 0)
	b.hashSet = set.NewStringSet()
	collator := collate.GetCollator(b.collation)
	for i := 1; i < len(b.args); i++ {
		if b.args[i].ConstItem(b.ctx.GetSessionVars().StmtCtx) {
			val, isNull, err := b.args[i].EvalString(ctx, chunk.Row{})
			if err != nil {
				return err
			}
			if isNull {
				b.hasNull = true
				continue
			}
			b.hashSet.Insert(string(collator.Key(val))) // should do memory copy here
		} else {
			b.nonConstArgsIdx = append(b.nonConstArgsIdx, i)
		}
	}

	return nil
}

func (b *builtinInStringSig) Clone() builtinFunc {
	newSig := &builtinInStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.nonConstArgsIdx = make([]int, len(b.nonConstArgsIdx))
	copy(newSig.nonConstArgsIdx, b.nonConstArgsIdx)
	newSig.hashSet = b.hashSet
	newSig.hasNull = b.hasNull
	return newSig
}

func (b *builtinInStringSig) evalInt(row chunk.Row) (int64, bool, error) {
	arg0, isNull0, err := b.args[0].EvalString(b.ctx, row)
	if isNull0 || err != nil {
		return 0, isNull0, err
	}

	args := b.args[1:]
	collator := collate.GetCollator(b.collation)
	if len(b.hashSet) != 0 {
		if b.hashSet.Exist(string(collator.Key(arg0))) {
			return 1, false, nil
		}
		args = make([]Expression, 0, len(b.nonConstArgsIdx))
		for _, i := range b.nonConstArgsIdx {
			args = append(args, b.args[i])
		}
	}

	hasNull := b.hasNull
	for _, arg := range args {
		evaledArg, isNull, err := arg.EvalString(b.ctx, row)
		if err != nil {
			return 0, true, err
		}
		if isNull {
			hasNull = true
			continue
		}
		if types.CompareString(arg0, evaledArg, b.collation) == 0 {
			return 1, false, nil
		}
	}
	return 0, hasNull, nil
}

// builtinInRealSig see https://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_in
type builtinInRealSig struct {
	baseInSig
	hashSet set.Float64Set
}

func (b *builtinInRealSig) buildHashMapForConstArgs(ctx sessionctx.Context) error {
	b.nonConstArgsIdx = make([]int, 0)
	b.hashSet = set.NewFloat64Set()
	for i := 1; i < len(b.args); i++ {
		if b.args[i].ConstItem(b.ctx.GetSessionVars().StmtCtx) {
			val, isNull, err := b.args[i].EvalReal(ctx, chunk.Row{})
			if err != nil {
				return err
			}
			if isNull {
				b.hasNull = true
				continue
			}
			b.hashSet.Insert(val)
		} else {
			b.nonConstArgsIdx = append(b.nonConstArgsIdx, i)
		}
	}

	return nil
}

func (b *builtinInRealSig) Clone() builtinFunc {
	newSig := &builtinInRealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.nonConstArgsIdx = make([]int, len(b.nonConstArgsIdx))
	copy(newSig.nonConstArgsIdx, b.nonConstArgsIdx)
	newSig.hashSet = b.hashSet
	newSig.hasNull = b.hasNull
	return newSig
}

func (b *builtinInRealSig) evalInt(row chunk.Row) (int64, bool, error) {
	arg0, isNull0, err := b.args[0].EvalReal(b.ctx, row)
	if isNull0 || err != nil {
		return 0, isNull0, err
	}
	args := b.args[1:]
	if len(b.hashSet) != 0 {
		if b.hashSet.Exist(arg0) {
			return 1, false, nil
		}
		args = make([]Expression, 0, len(b.nonConstArgsIdx))
		for _, i := range b.nonConstArgsIdx {
			args = append(args, b.args[i])
		}
	}

	hasNull := b.hasNull
	for _, arg := range args {
		evaledArg, isNull, err := arg.EvalReal(b.ctx, row)
		if err != nil {
			return 0, true, err
		}
		if isNull {
			hasNull = true
			continue
		}
		if arg0 == evaledArg {
			return 1, false, nil
		}
	}
	return 0, hasNull, nil
}

// builtinInDecimalSig see https://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_in
type builtinInDecimalSig struct {
	baseInSig
	hashSet set.StringSet
}

func (b *builtinInDecimalSig) buildHashMapForConstArgs(ctx sessionctx.Context) error {
	b.nonConstArgsIdx = make([]int, 0)
	b.hashSet = set.NewStringSet()
	for i := 1; i < len(b.args); i++ {
		if b.args[i].ConstItem(b.ctx.GetSessionVars().StmtCtx) {
			val, isNull, err := b.args[i].EvalDecimal(ctx, chunk.Row{})
			if err != nil {
				return err
			}
			if isNull {
				b.hasNull = true
				continue
			}
			key, err := val.ToHashKey()
			if err != nil {
				return err
			}
			b.hashSet.Insert(string(key))
		} else {
			b.nonConstArgsIdx = append(b.nonConstArgsIdx, i)
		}
	}

	return nil
}

func (b *builtinInDecimalSig) Clone() builtinFunc {
	newSig := &builtinInDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.nonConstArgsIdx = make([]int, len(b.nonConstArgsIdx))
	copy(newSig.nonConstArgsIdx, b.nonConstArgsIdx)
	newSig.hashSet = b.hashSet
	newSig.hasNull = b.hasNull
	return newSig
}

func (b *builtinInDecimalSig) evalInt(row chunk.Row) (int64, bool, error) {
	arg0, isNull0, err := b.args[0].EvalDecimal(b.ctx, row)
	if isNull0 || err != nil {
		return 0, isNull0, err
	}

	args := b.args[1:]
	key, err := arg0.ToHashKey()
	if err != nil {
		return 0, true, err
	}
	if len(b.hashSet) != 0 {
		if b.hashSet.Exist(string(key)) {
			return 1, false, nil
		}
		args = make([]Expression, 0, len(b.nonConstArgsIdx))
		for _, i := range b.nonConstArgsIdx {
			args = append(args, b.args[i])
		}
	}

	hasNull := b.hasNull
	for _, arg := range args {
		evaledArg, isNull, err := arg.EvalDecimal(b.ctx, row)
		if err != nil {
			return 0, true, err
		}
		if isNull {
			hasNull = true
			continue
		}
		if arg0.Compare(evaledArg) == 0 {
			return 1, false, nil
		}
	}
	return 0, hasNull, nil
}

// builtinInTimeSig see https://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_in
type builtinInTimeSig struct {
	baseInSig
	hashSet map[types.CoreTime]struct{}
}

func (b *builtinInTimeSig) buildHashMapForConstArgs(ctx sessionctx.Context) error {
	b.nonConstArgsIdx = make([]int, 0)
	b.hashSet = make(map[types.CoreTime]struct{}, len(b.args)-1)
	for i := 1; i < len(b.args); i++ {
		if b.args[i].ConstItem(b.ctx.GetSessionVars().StmtCtx) {
			val, isNull, err := b.args[i].EvalTime(ctx, chunk.Row{})
			if err != nil {
				return err
			}
			if isNull {
				b.hasNull = true
				continue
			}
			b.hashSet[val.CoreTime()] = struct{}{}
		} else {
			b.nonConstArgsIdx = append(b.nonConstArgsIdx, i)
		}
	}

	return nil
}

func (b *builtinInTimeSig) Clone() builtinFunc {
	newSig := &builtinInTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.nonConstArgsIdx = make([]int, len(b.nonConstArgsIdx))
	copy(newSig.nonConstArgsIdx, b.nonConstArgsIdx)
	newSig.hashSet = b.hashSet
	newSig.hasNull = b.hasNull
	return newSig
}

func (b *builtinInTimeSig) evalInt(row chunk.Row) (int64, bool, error) {
	arg0, isNull0, err := b.args[0].EvalTime(b.ctx, row)
	if isNull0 || err != nil {
		return 0, isNull0, err
	}
	args := b.args[1:]
	if len(b.hashSet) != 0 {
		if _, ok := b.hashSet[arg0.CoreTime()]; ok {
			return 1, false, nil
		}
		args = make([]Expression, 0, len(b.nonConstArgsIdx))
		for _, i := range b.nonConstArgsIdx {
			args = append(args, b.args[i])
		}
	}

	hasNull := b.hasNull
	for _, arg := range args {
		evaledArg, isNull, err := arg.EvalTime(b.ctx, row)
		if err != nil {
			return 0, true, err
		}
		if isNull {
			hasNull = true
			continue
		}
		if arg0.Compare(evaledArg) == 0 {
			return 1, false, nil
		}
	}
	return 0, hasNull, nil
}

// builtinInDurationSig see https://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_in
type builtinInDurationSig struct {
	baseInSig
	hashSet map[time.Duration]struct{}
}

func (b *builtinInDurationSig) buildHashMapForConstArgs(ctx sessionctx.Context) error {
	b.nonConstArgsIdx = make([]int, 0)
	b.hashSet = make(map[time.Duration]struct{}, len(b.args)-1)
	for i := 1; i < len(b.args); i++ {
		if b.args[i].ConstItem(b.ctx.GetSessionVars().StmtCtx) {
			val, isNull, err := b.args[i].EvalDuration(ctx, chunk.Row{})
			if err != nil {
				return err
			}
			if isNull {
				b.hasNull = true
				continue
			}
			b.hashSet[val.Duration] = struct{}{}
		} else {
			b.nonConstArgsIdx = append(b.nonConstArgsIdx, i)
		}
	}

	return nil
}

func (b *builtinInDurationSig) Clone() builtinFunc {
	newSig := &builtinInDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.nonConstArgsIdx = make([]int, len(b.nonConstArgsIdx))
	copy(newSig.nonConstArgsIdx, b.nonConstArgsIdx)
	newSig.hashSet = b.hashSet
	newSig.hasNull = b.hasNull
	return newSig
}

func (b *builtinInDurationSig) evalInt(row chunk.Row) (int64, bool, error) {
	arg0, isNull0, err := b.args[0].EvalDuration(b.ctx, row)
	if isNull0 || err != nil {
		return 0, isNull0, err
	}
	args := b.args[1:]
	if len(b.hashSet) != 0 {
		if _, ok := b.hashSet[arg0.Duration]; ok {
			return 1, false, nil
		}
		args = make([]Expression, 0, len(b.nonConstArgsIdx))
		for _, i := range b.nonConstArgsIdx {
			args = append(args, b.args[i])
		}
	}

	hasNull := b.hasNull
	for _, arg := range args {
		evaledArg, isNull, err := arg.EvalDuration(b.ctx, row)
		if err != nil {
			return 0, true, err
		}
		if isNull {
			hasNull = true
			continue
		}
		if arg0.Compare(evaledArg) == 0 {
			return 1, false, nil
		}
	}
	return 0, hasNull, nil
}

// builtinInJSONSig see https://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_in
type builtinInJSONSig struct {
	baseBuiltinFunc
}

func (b *builtinInJSONSig) Clone() builtinFunc {
	newSig := &builtinInJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinInJSONSig) evalInt(row chunk.Row) (int64, bool, error) {
	arg0, isNull0, err := b.args[0].EvalJSON(b.ctx, row)
	if isNull0 || err != nil {
		return 0, isNull0, err
	}
	var hasNull bool
	for _, arg := range b.args[1:] {
		evaledArg, isNull, err := arg.EvalJSON(b.ctx, row)
		if err != nil {
			return 0, true, err
		}
		if isNull {
			hasNull = true
			continue
		}
		result := json.CompareBinary(evaledArg, arg0)
		if result == 0 {
			return 1, false, nil
		}
	}
	return 0, hasNull, nil
}
