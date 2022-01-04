//
// This source code is a modified form of original source from the TiDB project, which has the following copyright header(s):
//

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
	goJSON "encoding/json"
	"fmt"
	"github.com/squareup/pranadb/tidb"
	"sync"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/opcode"
	//"github.com/squareup/pranadb/tidb/kv"
	"github.com/squareup/pranadb/tidb/sessionctx"
	"github.com/squareup/pranadb/tidb/sessionctx/stmtctx"
	"github.com/squareup/pranadb/tidb/types"
	"github.com/squareup/pranadb/tidb/types/json"
	"github.com/squareup/pranadb/tidb/util/chunk"
)

// These are byte flags used for `HashCode()`.
const (
	constantFlag       byte = 0
	columnFlag         byte = 1
	scalarFunctionFlag byte = 3
)

// VecExpr contains all vectorized evaluation methods.
type VecExpr interface {
	// Vectorized returns if this expression supports vectorized evaluation.
	Vectorized() bool

	// VecEvalInt evaluates this expression in a vectorized manner.
	VecEvalInt(ctx sessionctx.Context, input *chunk.Chunk, result *chunk.Column) error

	// VecEvalReal evaluates this expression in a vectorized manner.
	VecEvalReal(ctx sessionctx.Context, input *chunk.Chunk, result *chunk.Column) error

	// VecEvalString evaluates this expression in a vectorized manner.
	VecEvalString(ctx sessionctx.Context, input *chunk.Chunk, result *chunk.Column) error

	// VecEvalDecimal evaluates this expression in a vectorized manner.
	VecEvalDecimal(ctx sessionctx.Context, input *chunk.Chunk, result *chunk.Column) error

	// VecEvalTime evaluates this expression in a vectorized manner.
	VecEvalTime(ctx sessionctx.Context, input *chunk.Chunk, result *chunk.Column) error

	// VecEvalDuration evaluates this expression in a vectorized manner.
	VecEvalDuration(ctx sessionctx.Context, input *chunk.Chunk, result *chunk.Column) error

	// VecEvalJSON evaluates this expression in a vectorized manner.
	VecEvalJSON(ctx sessionctx.Context, input *chunk.Chunk, result *chunk.Column) error
}

// ReverseExpr contains all resersed evaluation methods.
type ReverseExpr interface {
	// SupportReverseEval checks whether the builtinFunc support reverse evaluation.
	SupportReverseEval() bool

	// ReverseEval evaluates the only one column value with given function result.
	ReverseEval(sc *stmtctx.StatementContext, res types.Datum, rType types.RoundingType) (val types.Datum, err error)
}

// Expression represents all scalar expression in SQL.
type Expression interface {
	fmt.Stringer
	goJSON.Marshaler
	VecExpr
	ReverseExpr
	CollationInfo

	// Eval evaluates an expression through a row.
	Eval(row chunk.Row) (types.Datum, error)

	// EvalInt returns the int64 representation of expression.
	EvalInt(ctx sessionctx.Context, row chunk.Row) (val int64, isNull bool, err error)

	// EvalReal returns the float64 representation of expression.
	EvalReal(ctx sessionctx.Context, row chunk.Row) (val float64, isNull bool, err error)

	// EvalString returns the string representation of expression.
	EvalString(ctx sessionctx.Context, row chunk.Row) (val string, isNull bool, err error)

	// EvalDecimal returns the decimal representation of expression.
	EvalDecimal(ctx sessionctx.Context, row chunk.Row) (val *types.MyDecimal, isNull bool, err error)

	// EvalTime returns the DATE/DATETIME/TIMESTAMP representation of expression.
	EvalTime(ctx sessionctx.Context, row chunk.Row) (val types.Time, isNull bool, err error)

	// EvalDuration returns the duration representation of expression.
	EvalDuration(ctx sessionctx.Context, row chunk.Row) (val types.Duration, isNull bool, err error)

	// EvalJSON returns the JSON representation of expression.
	EvalJSON(ctx sessionctx.Context, row chunk.Row) (val json.BinaryJSON, isNull bool, err error)

	// GetType gets the type that the expression returns.
	GetType() *types.FieldType

	// Clone copies an expression totally.
	Clone() Expression

	// Equal checks whether two expressions are equal.
	Equal(ctx sessionctx.Context, e Expression) bool

	// IsCorrelated checks if this expression has correlated key.
	IsCorrelated() bool

	// ConstItem checks if this expression is constant item, regardless of query evaluation state.
	// An expression is constant item if it:
	// refers no tables.
	// refers no correlated column.
	// refers no subqueries that refers any tables.
	// refers no non-deterministic functions.
	// refers no statement parameters.
	// refers no param markers when prepare plan cache is enabled.
	ConstItem(sc *stmtctx.StatementContext) bool

	// Decorrelate try to decorrelate the expression by schema.
	Decorrelate(schema *Schema) Expression

	// ResolveIndices resolves indices by the given schema. It will copy the original expression and return the copied one.
	ResolveIndices(schema *Schema) (Expression, error)

	// resolveIndices is called inside the `ResolveIndices` It will perform on the expression itself.
	resolveIndices(schema *Schema) error

	// HashCode creates the hashcode for expression which can be used to identify itself from other expression.
	// It generated as the following:
	// Constant: ConstantFlag+encoded value
	// Column: ColumnFlag+encoded value
	// ScalarFunction: SFFlag+encoded function name + encoded arg_1 + encoded arg_2 + ...
	HashCode(sc *stmtctx.StatementContext) []byte
}

// CNFExprs stands for a CNF expression.
type CNFExprs []Expression

// Clone clones itself.
func (e CNFExprs) Clone() CNFExprs {
	cnf := make(CNFExprs, 0, len(e))
	for _, expr := range e {
		cnf = append(cnf, expr.Clone())
	}
	return cnf
}

// Shallow makes a shallow copy of itself.
func (e CNFExprs) Shallow() CNFExprs {
	cnf := make(CNFExprs, 0, len(e))
	cnf = append(cnf, e...)
	return cnf
}

func isColumnInOperand(c *Column) bool {
	return c.InOperand
}

// IsEQCondFromIn checks if an expression is equal condition converted from `[not] in (subq)`.
func IsEQCondFromIn(expr Expression) bool {
	sf, ok := expr.(*ScalarFunction)
	if !ok || sf.FuncName.L != ast.EQ {
		return false
	}
	cols := make([]*Column, 0, 1)
	cols = ExtractColumnsFromExpressions(cols, sf.GetArgs(), isColumnInOperand)
	return len(cols) > 0
}

// HandleOverflowOnSelection handles Overflow errors when evaluating selection filters.
// We should ignore overflow errors when evaluating selection conditions:
//		INSERT INTO t VALUES ("999999999999999999");
//		SELECT * FROM t WHERE v;
func HandleOverflowOnSelection(sc *stmtctx.StatementContext, val int64, err error) (int64, error) {
	if err != nil && tidb.ErrOverflow.Equal(err) {
		return -1, nil
	}
	return val, err
}

// EvalBool evaluates expression list to a boolean value. The first returned value
// indicates bool result of the expression list, the second returned value indicates
// whether the result of the expression list is null, it can only be true when the
// first returned values is false.
func EvalBool(ctx sessionctx.Context, exprList CNFExprs, row chunk.Row) (bool, bool, error) {
	hasNull := false
	for _, expr := range exprList {
		data, err := expr.Eval(row)
		if err != nil {
			return false, false, err
		}
		if data.IsNull() {
			// For queries like `select a in (select a from s where t.b = s.b) from t`,
			// if result of `t.a = s.a` is null, we cannot return immediately until
			// we have checked if `t.b = s.b` is null or false, because it means
			// subquery is empty, and we should return false as the result of the whole
			// exprList in that case, instead of null.
			if !IsEQCondFromIn(expr) {
				return false, false, nil
			}
			hasNull = true
			continue
		}

		i, err := data.ToBool(ctx.GetSessionVars().StmtCtx)
		if err != nil {
			i, err = HandleOverflowOnSelection(ctx.GetSessionVars().StmtCtx, i, err)
			if err != nil {
				return false, false, err

			}
		}
		if i == 0 {
			return false, false, nil
		}
	}
	if hasNull {
		return false, true, nil
	}
	return true, false, nil
}

var (
	defaultChunkSize = 1024
	selPool          = sync.Pool{
		New: func() interface{} {
			return make([]int, defaultChunkSize)
		},
	}
	zeroPool = sync.Pool{
		New: func() interface{} {
			return make([]int8, defaultChunkSize)
		},
	}
)

// composeConditionWithBinaryOp composes condition with binary operator into a balance deep tree, which benefits a lot for pb decoder/encoder.
func composeConditionWithBinaryOp(ctx sessionctx.Context, conditions []Expression, funcName string) Expression {
	length := len(conditions)
	if length == 0 {
		return nil
	}
	if length == 1 {
		return conditions[0]
	}
	expr := NewFunctionInternal(ctx, funcName,
		types.NewFieldType(mysql.TypeTiny),
		composeConditionWithBinaryOp(ctx, conditions[:length/2], funcName),
		composeConditionWithBinaryOp(ctx, conditions[length/2:], funcName))
	return expr
}

// ComposeCNFCondition composes CNF items into a balance deep CNF tree, which benefits a lot for pb decoder/encoder.
func ComposeCNFCondition(ctx sessionctx.Context, conditions ...Expression) Expression {
	return composeConditionWithBinaryOp(ctx, conditions, ast.LogicAnd)
}

// ComposeDNFCondition composes DNF items into a balance deep DNF tree.
func ComposeDNFCondition(ctx sessionctx.Context, conditions ...Expression) Expression {
	return composeConditionWithBinaryOp(ctx, conditions, ast.LogicOr)
}

func extractBinaryOpItems(conditions *ScalarFunction, funcName string) []Expression {
	var ret []Expression
	for _, arg := range conditions.GetArgs() {
		if sf, ok := arg.(*ScalarFunction); ok && sf.FuncName.L == funcName {
			ret = append(ret, extractBinaryOpItems(sf, funcName)...)
		} else {
			ret = append(ret, arg)
		}
	}
	return ret
}

// FlattenDNFConditions extracts DNF expression's leaf item.
// e.g. or(or(a=1, a=2), or(a=3, a=4)), we'll get [a=1, a=2, a=3, a=4].
func FlattenDNFConditions(DNFCondition *ScalarFunction) []Expression {
	return extractBinaryOpItems(DNFCondition, ast.LogicOr)
}

// FlattenCNFConditions extracts CNF expression's leaf item.
// e.g. and(and(a>1, a>2), and(a>3, a>4)), we'll get [a>1, a>2, a>3, a>4].
func FlattenCNFConditions(CNFCondition *ScalarFunction) []Expression {
	return extractBinaryOpItems(CNFCondition, ast.LogicAnd)
}

// Assignment represents a set assignment in Update, such as
// Update t set c1 = hex(12), c2 = c3 where c2 = 1
type Assignment struct {
	Col *Column
	// ColName indicates its original column name in table schema. It's used for outputting helping message when executing meets some errors.
	ColName model.CIStr
	Expr    Expression
}

// VarAssignment represents a variable assignment in Set, such as set global a = 1.
type VarAssignment struct {
	Name        string
	Expr        Expression
	IsDefault   bool
	IsGlobal    bool
	IsSystem    bool
	ExtendValue *Constant
}

// splitNormalFormItems split CNF(conjunctive normal form) like "a and b and c", or DNF(disjunctive normal form) like "a or b or c"
func splitNormalFormItems(onExpr Expression, funcName string) []Expression {
	switch v := onExpr.(type) {
	case *ScalarFunction:
		if v.FuncName.L == funcName {
			var ret []Expression
			for _, arg := range v.GetArgs() {
				ret = append(ret, splitNormalFormItems(arg, funcName)...)
			}
			return ret
		}
	}
	return []Expression{onExpr}
}

// SplitCNFItems splits CNF items.
// CNF means conjunctive normal form, e.g. "a and b and c".
func SplitCNFItems(onExpr Expression) []Expression {
	return splitNormalFormItems(onExpr, ast.LogicAnd)
}

// SplitDNFItems splits DNF items.
// DNF means disjunctive normal form, e.g. "a or b or c".
func SplitDNFItems(onExpr Expression) []Expression {
	return splitNormalFormItems(onExpr, ast.LogicOr)
}

// EvaluateExprWithNull sets columns in schema as null and calculate the final result of the scalar function.
// If the Expression is a non-constant value, it means the result is unknown.
func EvaluateExprWithNull(ctx sessionctx.Context, schema *Schema, expr Expression) Expression {
	if ContainMutableConst(ctx, []Expression{expr}) {
		ctx.GetSessionVars().StmtCtx.OptimDependOnMutableConst = true
	}
	return evaluateExprWithNull(ctx, schema, expr)
}

func evaluateExprWithNull(ctx sessionctx.Context, schema *Schema, expr Expression) Expression {
	switch x := expr.(type) {
	case *ScalarFunction:
		args := make([]Expression, len(x.GetArgs()))
		for i, arg := range x.GetArgs() {
			args[i] = evaluateExprWithNull(ctx, schema, arg)
		}
		return NewFunctionInternal(ctx, x.FuncName.L, x.RetType, args...)
	case *Column:
		if !schema.Contains(x) {
			return x
		}
		return &Constant{Value: types.Datum{}, RetType: types.NewFieldType(mysql.TypeNull)}
	case *Constant:
		if x.DeferredExpr != nil {
			return FoldConstant(x)
		}
	}
	return expr
}

// IsBinaryLiteral checks whether an expression is a binary literal
func IsBinaryLiteral(expr Expression) bool {
	con, ok := expr.(*Constant)
	return ok && con.Value.Kind() == types.KindBinaryLiteral
}

// PushDownExprs split the input exprs into pushed and remained, pushed include all the exprs that can be pushed down
func PushDownExprs(sc *stmtctx.StatementContext, exprs []Expression) (pushed []Expression, remained []Expression) {
	for _, expr := range exprs {
		pushed = append(pushed, expr)
	}
	return
}

// CanExprsPushDown return true if all the expr in exprs can be pushed down
func CanExprsPushDown(sc *stmtctx.StatementContext, exprs []Expression) bool {
	_, remained := PushDownExprs(sc, exprs)
	return len(remained) == 0
}

// wrapWithIsTrue wraps `arg` with istrue function if the return type of expr is not
// type int, otherwise, returns `arg` directly.
// The `keepNull` controls what the istrue function will return when `arg` is null:
// 1. keepNull is true and arg is null, the istrue function returns null.
// 2. keepNull is false and arg is null, the istrue function returns 0.
// The `wrapForInt` indicates whether we need to wrapIsTrue for non-logical Expression with int type.
// TODO: remove this function. ScalarFunction should be newed in one place.
func wrapWithIsTrue(ctx sessionctx.Context, keepNull bool, arg Expression, wrapForInt bool) (Expression, error) {
	if arg.GetType().EvalType() == types.ETInt {
		if !wrapForInt {
			return arg, nil
		}
		if child, ok := arg.(*ScalarFunction); ok {
			if _, isLogicalOp := logicalOps[child.FuncName.L]; isLogicalOp {
				return arg, nil
			}
		}
	}
	var fc *isTrueOrFalseFunctionClass
	if keepNull {
		fc = &isTrueOrFalseFunctionClass{baseFunctionClass{ast.IsTruthWithNull, 1, 1}, opcode.IsTruth, keepNull}
	} else {
		fc = &isTrueOrFalseFunctionClass{baseFunctionClass{ast.IsTruthWithoutNull, 1, 1}, opcode.IsTruth, keepNull}
	}
	f, err := fc.getFunction(ctx, []Expression{arg})
	if err != nil {
		return nil, err
	}
	sf := &ScalarFunction{
		FuncName: model.NewCIStr(ast.IsTruthWithoutNull),
		Function: f,
		RetType:  f.getRetTp(),
	}
	if keepNull {
		sf.FuncName = model.NewCIStr(ast.IsTruthWithNull)
	}
	return FoldConstant(sf), nil
}
