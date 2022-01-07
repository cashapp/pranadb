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

package planner

import (
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/opcode"
	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/tidb"
	"github.com/squareup/pranadb/tidb/expression"
	"github.com/squareup/pranadb/tidb/sessionctx"
	"github.com/squareup/pranadb/tidb/types"
	driver "github.com/squareup/pranadb/tidb/types/parser_driver"
	"github.com/squareup/pranadb/tidb/util/chunk"
	"github.com/squareup/pranadb/tidb/util/collate"
	"github.com/squareup/pranadb/tidb/util/stringutil"
	"strconv"
)

// rewrite function rewrites ast expr to expression.Expression.
// aggMapper maps ast.AggregateFuncExpr to the columns offset in p's output schema.
// asScalar means whether this expression must be treated as a scalar expression.
// And this function returns a result expression, a new plan that may have apply or semi-join.
func (b *PlanBuilder) rewrite(exprNode ast.ExprNode, p LogicalPlan, aggMapper map[*ast.AggregateFuncExpr]int, asScalar bool) (expression.Expression, LogicalPlan, error) {
	expr, resultPlan, err := b.rewriteWithPreprocess(exprNode, p, aggMapper, nil, asScalar, nil)
	return expr, resultPlan, err
}

// rewriteWithPreprocess is for handling the situation that we need to adjust the input ast tree
// before really using its node in `expressionRewriter.Leave`. In that case, we first call
// er.preprocess(expr), which returns a new expr. Then we use the new expr in `Leave`.
func (b *PlanBuilder) rewriteWithPreprocess(
	exprNode ast.ExprNode,
	p LogicalPlan, aggMapper map[*ast.AggregateFuncExpr]int,
	windowMapper map[*ast.WindowFuncExpr]int,
	asScalar bool,
	preprocess func(ast.Node) ast.Node,
) (expression.Expression, LogicalPlan, error) {
	b.rewriterCounter++
	defer func() { b.rewriterCounter-- }()

	rewriter := b.getExpressionRewriter(p)
	// The rewriter maybe is obtained from "b.rewriterPool", "rewriter.err" is
	// not nil means certain previous procedure has not handled this error.
	// Here we give us one more chance to make a correct behavior by handling
	// this missed error.
	if rewriter.err != nil {
		return nil, nil, rewriter.err
	}

	rewriter.aggrMap = aggMapper
	rewriter.windowMap = windowMapper
	rewriter.asScalar = asScalar
	rewriter.preprocess = preprocess

	expr, resultPlan, err := b.rewriteExprNode(rewriter, exprNode, asScalar)
	return expr, resultPlan, err
}

func (b *PlanBuilder) getExpressionRewriter(p LogicalPlan) (rewriter *expressionRewriter) {
	defer func() {
		if p != nil {
			rewriter.schema = p.Schema()
			rewriter.names = p.OutputNames()
		}
	}()

	if len(b.rewriterPool) < b.rewriterCounter {
		rewriter = &expressionRewriter{p: p, b: b, sctx: b.ctx}
		b.rewriterPool = append(b.rewriterPool, rewriter)
		return
	}

	rewriter = b.rewriterPool[b.rewriterCounter-1]
	rewriter.p = p
	rewriter.asScalar = false
	rewriter.aggrMap = nil
	rewriter.preprocess = nil
	rewriter.disableFoldCounter = 0
	rewriter.tryFoldCounter = 0
	rewriter.ctxStack = rewriter.ctxStack[:0]
	rewriter.ctxNameStk = rewriter.ctxNameStk[:0]
	rewriter.err = nil
	return
}

func (b *PlanBuilder) rewriteExprNode(rewriter *expressionRewriter, exprNode ast.ExprNode, asScalar bool) (expression.Expression, LogicalPlan, error) {
	if rewriter.p != nil {
		curColLen := rewriter.p.Schema().Len()
		defer func() {
			names := rewriter.p.OutputNames().Shallow()[:curColLen]
			for i := curColLen; i < rewriter.p.Schema().Len(); i++ {
				names = append(names, types.EmptyName)
			}
			// After rewriting finished, only old columns are visible.
			// e.g. select * from t where t.a in (select t1.a from t1);
			// The output columns before we enter the subquery are the columns from t.
			// But when we leave the subquery `t.a in (select t1.a from t1)`, we got a Apply operator
			// and the output columns become [t.*, t1.*]. But t1.* is used only inside the subquery. If there's another filter
			// which is also a subquery where t1 is involved. The name resolving will fail if we still expose the column from
			// the previous subquery.
			// So here we just reset the names to empty to avoid this situation.
			// TODO: implement ScalarSubQuery and resolve it during optimizing. In building phase, we will not change the plan's structure.
			rewriter.p.SetOutputNames(names)
		}()
	}
	exprNode.Accept(rewriter)
	if rewriter.err != nil {
		return nil, nil, errors.Trace(rewriter.err)
	}
	if !asScalar && len(rewriter.ctxStack) == 0 {
		return nil, rewriter.p, nil
	}
	if len(rewriter.ctxStack) != 1 {
		return nil, nil, errors.Errorf("context len %v is invalid", len(rewriter.ctxStack))
	}
	rewriter.err = expression.CheckArgsNotMultiColumnRow(rewriter.ctxStack[0])
	if rewriter.err != nil {
		return nil, nil, errors.Trace(rewriter.err)
	}
	return rewriter.ctxStack[0], rewriter.p, nil
}

type expressionRewriter struct {
	ctxStack   []expression.Expression
	ctxNameStk []*types.FieldName
	p          LogicalPlan
	schema     *expression.Schema
	names      []*types.FieldName
	err        error
	aggrMap    map[*ast.AggregateFuncExpr]int
	windowMap  map[*ast.WindowFuncExpr]int
	b          *PlanBuilder
	sctx       sessionctx.Context

	// asScalar indicates the return value must be a scalar value.
	// NOTE: This value can be changed during expression rewritten.
	asScalar bool

	// preprocess is called for every ast.Node in Leave.
	preprocess func(ast.Node) ast.Node

	// disableFoldCounter controls fold-disabled scope. If > 0, rewriter will NOT do constant folding.
	// Typically, during visiting AST, while entering the scope(disable), the counter will +1; while
	// leaving the scope(enable again), the counter will -1.
	// NOTE: This value can be changed during expression rewritten.
	disableFoldCounter int
	tryFoldCounter     int
}

func (er *expressionRewriter) ctxStackLen() int {
	return len(er.ctxStack)
}

func (er *expressionRewriter) ctxStackPop(num int) {
	l := er.ctxStackLen()
	er.ctxStack = er.ctxStack[:l-num]
	er.ctxNameStk = er.ctxNameStk[:l-num]
}

func (er *expressionRewriter) ctxStackAppend(col expression.Expression, name *types.FieldName) {
	er.ctxStack = append(er.ctxStack, col)
	er.ctxNameStk = append(er.ctxNameStk, name)
}

// constructBinaryOpFunction converts binary operator functions
// 1. If op are EQ or NE or NullEQ, constructBinaryOpFunctions converts (a0,a1,a2) op (b0,b1,b2) to (a0 op b0) and (a1 op b1) and (a2 op b2)
// 2. Else constructBinaryOpFunctions converts (a0,a1,a2) op (b0,b1,b2) to
// `IF( a0 NE b0, a0 op b0,
// 		IF ( isNull(a0 NE b0), Null,
// 			IF ( a1 NE b1, a1 op b1,
// 				IF ( isNull(a1 NE b1), Null, a2 op b2))))`
func (er *expressionRewriter) constructBinaryOpFunction(l expression.Expression, r expression.Expression, op string) (expression.Expression, error) {
	lLen, rLen := expression.GetRowLen(l), expression.GetRowLen(r)
	if lLen == 1 && rLen == 1 {
		return er.newFunction(op, types.NewFieldType(mysql.TypeTiny), l, r)
	} else if rLen != lLen {
		return nil, tidb.ErrOperandColumns.GenWithStackByArgs(lLen)
	}
	switch op {
	case ast.EQ, ast.NE, ast.NullEQ:
		funcs := make([]expression.Expression, lLen)
		for i := 0; i < lLen; i++ {
			var err error
			funcs[i], err = er.constructBinaryOpFunction(expression.GetFuncArg(l, i), expression.GetFuncArg(r, i), op)
			if err != nil {
				return nil, err
			}
		}
		if op == ast.NE {
			return expression.ComposeDNFCondition(er.sctx, funcs...), nil
		}
		return expression.ComposeCNFCondition(er.sctx, funcs...), nil
	default:
		larg0, rarg0 := expression.GetFuncArg(l, 0), expression.GetFuncArg(r, 0)
		var expr1, expr2, expr3, expr4, expr5 expression.Expression
		expr1 = expression.NewFunctionInternal(er.sctx, ast.NE, types.NewFieldType(mysql.TypeTiny), larg0, rarg0)
		expr2 = expression.NewFunctionInternal(er.sctx, op, types.NewFieldType(mysql.TypeTiny), larg0, rarg0)
		expr3 = expression.NewFunctionInternal(er.sctx, ast.IsNull, types.NewFieldType(mysql.TypeTiny), expr1)
		var err error
		l, err = expression.PopRowFirstArg(er.sctx, l)
		if err != nil {
			return nil, err
		}
		r, err = expression.PopRowFirstArg(er.sctx, r)
		if err != nil {
			return nil, err
		}
		expr4, err = er.constructBinaryOpFunction(l, r, op)
		if err != nil {
			return nil, err
		}
		expr5, err = er.newFunction(ast.If, types.NewFieldType(mysql.TypeTiny), expr3, expression.NewNull(), expr4)
		if err != nil {
			return nil, err
		}
		return er.newFunction(ast.If, types.NewFieldType(mysql.TypeTiny), expr1, expr2, expr5)
	}
}

func (er *expressionRewriter) buildSubquery(subq *ast.SubqueryExpr) (LogicalPlan, error) {
	if er.schema != nil {
		outerSchema := er.schema.Clone()
		er.b.outerSchemas = append(er.b.outerSchemas, outerSchema)
		er.b.outerNames = append(er.b.outerNames, er.names)
		defer func() {
			er.b.outerSchemas = er.b.outerSchemas[0 : len(er.b.outerSchemas)-1]
			er.b.outerNames = er.b.outerNames[0 : len(er.b.outerNames)-1]
		}()
	}

	np, err := er.b.buildResultSetNode(subq.Query)
	if err != nil {
		return nil, err
	}
	// Pop the handle map generated by the subquery.
	er.b.handleHelper.popMap()
	return np, nil
}

// Enter implements Visitor interface.
func (er *expressionRewriter) Enter(inNode ast.Node) (ast.Node, bool) {
	switch v := inNode.(type) {
	case *ast.AggregateFuncExpr:
		index, ok := -1, false
		if er.aggrMap != nil {
			index, ok = er.aggrMap[v]
		}
		if ok {
			// index < 0 indicates this is a correlated aggregate belonging to outer query,
			// for which a correlated column will be created later, so we append a null constant
			// as a temporary result expression.
			if index < 0 {
				er.ctxStackAppend(expression.NewNull(), types.EmptyName)
			} else {
				// index >= 0 indicates this is a regular aggregate column
				er.ctxStackAppend(er.schema.Columns[index], er.names[index])
			}
			return inNode, true
		}
		// replace correlated aggregate in sub-query with its corresponding correlated column
		if col, ok := er.b.correlatedAggMapper[v]; ok {
			er.ctxStackAppend(col, types.EmptyName)
			return inNode, true
		}
		er.err = tidb.ErrInvalidGroupFuncUse
		return inNode, true
	case *ast.ColumnNameExpr:
		if index, ok := er.b.colMapper[v]; ok {
			er.ctxStackAppend(er.schema.Columns[index], er.names[index])
			return inNode, true
		}
	case *ast.PatternInExpr:
		if len(v.List) != 1 {
			break
		}
		// For 10 in ((select * from t)), the parser won't set v.Sel.
		// So we must process this case here.
		x := v.List[0]
		for {
			switch y := x.(type) {
			case *ast.ParenthesesExpr:
				x = y.Expr
			default:
				return inNode, false
			}
		}
	case *ast.ParenthesesExpr:
	case *ast.FuncCallExpr:
		er.asScalar = true
		if _, ok := expression.DisableFoldFunctions[v.FnName.L]; ok {
			er.disableFoldCounter++
		}
		if _, ok := expression.TryFoldFunctions[v.FnName.L]; ok {
			er.tryFoldCounter++
		}
	case *ast.CaseExpr:
		er.asScalar = true
		if _, ok := expression.DisableFoldFunctions["case"]; ok {
			er.disableFoldCounter++
		}
		if _, ok := expression.TryFoldFunctions["case"]; ok {
			er.tryFoldCounter++
		}
	case *ast.BinaryOperationExpr:
		er.asScalar = true
		if v.Op == opcode.LogicAnd || v.Op == opcode.LogicOr {
			er.tryFoldCounter++
		}
	case *ast.SetCollationExpr:
		// Do nothing
	default:
		er.asScalar = true
	}
	return inNode, false
}

// Leave implements Visitor interface.
func (er *expressionRewriter) Leave(originInNode ast.Node) (retNode ast.Node, ok bool) {
	if er.err != nil {
		return retNode, false
	}
	var inNode = originInNode
	if er.preprocess != nil {
		inNode = er.preprocess(inNode)
	}
	switch v := inNode.(type) {
	case *ast.AggregateFuncExpr, *ast.ColumnNameExpr, *ast.ParenthesesExpr, *ast.WhenClause,
		*ast.SubqueryExpr, *ast.ExistsSubqueryExpr, *ast.CompareSubqueryExpr, *ast.ValuesExpr, *ast.WindowFuncExpr, *ast.TableNameExpr:
	case *driver.ValueExpr:
		// set right not null flag for constant value
		retType := v.Type.Clone()
		switch v.Datum.Kind() {
		case types.KindNull:
			retType.Flag &= ^mysql.NotNullFlag
		default:
			retType.Flag |= mysql.NotNullFlag
		}
		v.Datum.SetValue(v.Datum.GetValue(), retType)
		value := &expression.Constant{Value: v.Datum, RetType: retType}
		er.ctxStackAppend(value, types.EmptyName)
	case *driver.ParamMarkerExpr:
		var value expression.Expression
		value, er.err = expression.ParamMarkerExpression(er.sctx, v)
		if er.err != nil {
			return retNode, false
		}
		er.ctxStackAppend(value, types.EmptyName)
	case *ast.FuncCallExpr:
		if _, ok := expression.TryFoldFunctions[v.FnName.L]; ok {
			er.tryFoldCounter--
		}
		er.funcCallToExpression(v)
		if _, ok := expression.DisableFoldFunctions[v.FnName.L]; ok {
			er.disableFoldCounter--
		}
	case *ast.TableName:
		er.toTable(v)
	case *ast.ColumnName:
		er.toColumn(v)
	case *ast.UnaryOperationExpr:
		er.unaryOpToExpression(v)
	case *ast.BinaryOperationExpr:
		if v.Op == opcode.LogicAnd || v.Op == opcode.LogicOr {
			er.tryFoldCounter--
		}
		er.binaryOpToExpression(v)
	case *ast.BetweenExpr:
		er.betweenToExpression(v)
	case *ast.CaseExpr:
		if _, ok := expression.TryFoldFunctions["case"]; ok {
			er.tryFoldCounter--
		}
		er.caseToExpression(v)
		if _, ok := expression.DisableFoldFunctions["case"]; ok {
			er.disableFoldCounter--
		}
	case *ast.FuncCastExpr:
		arg := er.ctxStack[len(er.ctxStack)-1]
		er.err = expression.CheckArgsNotMultiColumnRow(arg)
		if er.err != nil {
			return retNode, false
		}

		// check the decimal precision of "CAST(AS TIME)".
		er.err = er.checkTimePrecision(v.Tp)
		if er.err != nil {
			return retNode, false
		}

		if v.Tp.EvalType() == types.ETString {
			arg.SetCoercibility(expression.CoercibilityImplicit)
		}

		er.ctxStack[len(er.ctxStack)-1] = expression.BuildCastFunction(er.sctx, arg, v.Tp)
		er.ctxNameStk[len(er.ctxNameStk)-1] = types.EmptyName
	case *ast.PatternLikeExpr:
		er.patternLikeToExpression(v)
	case *ast.PatternRegexpExpr:
		er.regexpToScalarFunc(v)
	case *ast.RowExpr:
		er.rowToScalarFunc(v)
	case *ast.PatternInExpr:
		if v.Sel == nil {
			er.inToExpression(len(v.List), v.Not, &v.Type)
		}
	case *ast.PositionExpr:
		er.positionToScalarFunc(v)
	case *ast.IsNullExpr:
		er.isNullToExpression(v)
	case *ast.IsTruthExpr:
		er.isTrueToScalarFunc(v)
	// TODO: Perhaps we don't need to transcode these back to generic integers/strings
	case *ast.TrimDirectionExpr:
		er.ctxStackAppend(&expression.Constant{
			Value:   types.NewIntDatum(int64(v.Direction)),
			RetType: types.NewFieldType(mysql.TypeTiny),
		}, types.EmptyName)
	case *ast.TimeUnitExpr:
		er.ctxStackAppend(&expression.Constant{
			Value:   types.NewStringDatum(v.Unit.String()),
			RetType: types.NewFieldType(mysql.TypeVarchar),
		}, types.EmptyName)
	case *ast.GetFormatSelectorExpr:
		er.ctxStackAppend(&expression.Constant{
			Value:   types.NewStringDatum(v.Selector.String()),
			RetType: types.NewFieldType(mysql.TypeVarchar),
		}, types.EmptyName)
	case *ast.SetCollationExpr:
		arg := er.ctxStack[len(er.ctxStack)-1]
		if collate.NewCollationEnabled() {
			var collInfo *charset.Collation
			// TODO(bb7133): use charset.ValidCharsetAndCollation when its bug is fixed.
			if collInfo, er.err = collate.GetCollationByName(v.Collate); er.err != nil {
				break
			}
			chs := arg.GetType().Charset
			if chs != "" && collInfo.CharsetName != chs {
				er.err = charset.ErrCollationCharsetMismatch.GenWithStackByArgs(collInfo.Name, chs)
				break
			}
		}
		// SetCollationExpr sets the collation explicitly, even when the evaluation type of the expression is non-string.
		if _, ok := arg.(*expression.Column); ok {
			// Wrap a cast here to avoid changing the original FieldType of the column expression.
			exprType := arg.GetType().Clone()
			exprType.Collate = v.Collate
			casted := expression.BuildCastFunction(er.sctx, arg, exprType)
			er.ctxStackPop(1)
			er.ctxStackAppend(casted, types.EmptyName)
		} else {
			// For constant and scalar function, we can set its collate directly.
			arg.GetType().Collate = v.Collate
		}
		er.ctxStack[len(er.ctxStack)-1].SetCoercibility(expression.CoercibilityExplicit)
		er.ctxStack[len(er.ctxStack)-1].SetCharsetAndCollation(arg.GetType().Charset, arg.GetType().Collate)
	default:
		er.err = errors.Errorf("UnknownType: %T", v)
		return retNode, false
	}

	if er.err != nil {
		return retNode, false
	}
	return originInNode, true
}

// newFunction chooses which expression.NewFunctionImpl() will be used.
func (er *expressionRewriter) newFunction(funcName string, retType *types.FieldType, args ...expression.Expression) (expression.Expression, error) {
	if er.disableFoldCounter > 0 {
		return expression.NewFunctionBase(er.sctx, funcName, retType, args...)
	}
	if er.tryFoldCounter > 0 {
		return expression.NewFunctionTryFold(er.sctx, funcName, retType, args...)
	}
	return expression.NewFunction(er.sctx, funcName, retType, args...)
}

func (er *expressionRewriter) checkTimePrecision(ft *types.FieldType) error {
	if ft.EvalType() == types.ETDuration && ft.Decimal > int(types.MaxFsp) {
		return tidb.ErrTooBigPrecision.GenWithStackByArgs(ft.Decimal, "CAST", types.MaxFsp)
	}
	return nil
}

func (er *expressionRewriter) useCache() bool {
	return er.sctx.GetSessionVars().StmtCtx.UseCache
}

func (er *expressionRewriter) unaryOpToExpression(v *ast.UnaryOperationExpr) {
	stkLen := len(er.ctxStack)
	var op string
	switch v.Op {
	case opcode.Plus:
		// expression (+ a) is equal to a
		return
	case opcode.Minus:
		op = ast.UnaryMinus
	case opcode.BitNeg:
		op = ast.BitNeg
	case opcode.Not, opcode.Not2:
		op = ast.UnaryNot
	default:
		er.err = errors.Errorf("Unknown Unary Op %T", v.Op)
		return
	}
	if expression.GetRowLen(er.ctxStack[stkLen-1]) != 1 {
		er.err = tidb.ErrOperandColumns.GenWithStackByArgs(1)
		return
	}
	er.ctxStack[stkLen-1], er.err = er.newFunction(op, &v.Type, er.ctxStack[stkLen-1])
	er.ctxNameStk[stkLen-1] = types.EmptyName
}

func (er *expressionRewriter) binaryOpToExpression(v *ast.BinaryOperationExpr) {
	stkLen := len(er.ctxStack)
	var function expression.Expression
	switch v.Op {
	case opcode.EQ, opcode.NE, opcode.NullEQ, opcode.GT, opcode.GE, opcode.LT, opcode.LE:
		function, er.err = er.constructBinaryOpFunction(er.ctxStack[stkLen-2], er.ctxStack[stkLen-1],
			v.Op.String())
	default:
		lLen := expression.GetRowLen(er.ctxStack[stkLen-2])
		rLen := expression.GetRowLen(er.ctxStack[stkLen-1])
		if lLen != 1 || rLen != 1 {
			er.err = tidb.ErrOperandColumns.GenWithStackByArgs(1)
			return
		}
		function, er.err = er.newFunction(v.Op.String(), types.NewFieldType(mysql.TypeUnspecified), er.ctxStack[stkLen-2:]...)
	}
	if er.err != nil {
		return
	}
	er.ctxStackPop(2)
	er.ctxStackAppend(function, types.EmptyName)
}

func (er *expressionRewriter) notToExpression(hasNot bool, op string, tp *types.FieldType,
	args ...expression.Expression) expression.Expression {
	opFunc, err := er.newFunction(op, tp, args...)
	if err != nil {
		er.err = err
		return nil
	}
	if !hasNot {
		return opFunc
	}

	opFunc, err = er.newFunction(ast.UnaryNot, tp, opFunc)
	if err != nil {
		er.err = err
		return nil
	}
	return opFunc
}

func (er *expressionRewriter) isNullToExpression(v *ast.IsNullExpr) {
	stkLen := len(er.ctxStack)
	if expression.GetRowLen(er.ctxStack[stkLen-1]) != 1 {
		er.err = tidb.ErrOperandColumns.GenWithStackByArgs(1)
		return
	}
	function := er.notToExpression(v.Not, ast.IsNull, &v.Type, er.ctxStack[stkLen-1])
	er.ctxStackPop(1)
	er.ctxStackAppend(function, types.EmptyName)
}

func (er *expressionRewriter) positionToScalarFunc(v *ast.PositionExpr) {
	pos := v.N
	str := strconv.Itoa(pos)
	if v.P != nil {
		stkLen := len(er.ctxStack)
		val := er.ctxStack[stkLen-1]
		intNum, isNull, err := expression.GetIntFromConstant(er.sctx, val)
		str = "?"
		if err == nil {
			if isNull {
				return
			}
			pos = intNum
			er.ctxStackPop(1)
		}
		er.err = err
	}
	if er.err == nil && pos > 0 && pos <= er.schema.Len() {
		er.ctxStackAppend(er.schema.Columns[pos-1], er.names[pos-1])
	} else {
		er.err = tidb.ErrUnknownColumn.GenWithStackByArgs(str, clauseMsg[er.b.curClause])
	}
}

func (er *expressionRewriter) isTrueToScalarFunc(v *ast.IsTruthExpr) {
	stkLen := len(er.ctxStack)
	op := ast.IsTruthWithoutNull
	if v.True == 0 {
		op = ast.IsFalsity
	}
	if expression.GetRowLen(er.ctxStack[stkLen-1]) != 1 {
		er.err = tidb.ErrOperandColumns.GenWithStackByArgs(1)
		return
	}
	function := er.notToExpression(v.Not, op, &v.Type, er.ctxStack[stkLen-1])
	er.ctxStackPop(1)
	er.ctxStackAppend(function, types.EmptyName)
}

// inToExpression converts in expression to a scalar function. The argument lLen means the length of in list.
// The argument not means if the expression is not in. The tp stands for the expression type, which is always bool.
// a in (b, c, d) will be rewritten as `(a = b) or (a = c) or (a = d)`.
func (er *expressionRewriter) inToExpression(lLen int, not bool, tp *types.FieldType) {
	stkLen := len(er.ctxStack)
	l := expression.GetRowLen(er.ctxStack[stkLen-lLen-1])
	for i := 0; i < lLen; i++ {
		if l != expression.GetRowLen(er.ctxStack[stkLen-lLen+i]) {
			er.err = tidb.ErrOperandColumns.GenWithStackByArgs(l)
			return
		}
	}
	args := er.ctxStack[stkLen-lLen-1:]
	leftFt := args[0].GetType()
	leftEt, leftIsNull := leftFt.EvalType(), leftFt.Tp == mysql.TypeNull
	if leftIsNull {
		er.ctxStackPop(lLen + 1)
		er.ctxStackAppend(expression.NewNull(), types.EmptyName)
		return
	}
	containMut := expression.ContainMutableConst(er.sctx, args)
	if !containMut && leftEt == types.ETInt {
		for i := 1; i < len(args); i++ {
			if c, ok := args[i].(*expression.Constant); ok {
				var isExceptional bool
				args[i], isExceptional = expression.RefineComparedConstant(er.sctx, *leftFt, c, opcode.EQ)
				if isExceptional {
					args[i] = c
				}
			}
		}
	}
	allSameType := true
	for _, arg := range args[1:] {
		if arg.GetType().Tp != mysql.TypeNull && expression.GetAccurateCmpType(args[0], arg) != leftEt {
			allSameType = false
			break
		}
	}
	var function expression.Expression
	if allSameType && l == 1 && lLen > 1 {
		function = er.notToExpression(not, ast.In, tp, er.ctxStack[stkLen-lLen-1:]...)
	} else {
		eqFunctions := make([]expression.Expression, 0, lLen)
		for i := stkLen - lLen; i < stkLen; i++ {
			expr, err := er.constructBinaryOpFunction(args[0], er.ctxStack[i], ast.EQ)
			if err != nil {
				er.err = err
				return
			}
			eqFunctions = append(eqFunctions, expr)
		}
		function = expression.ComposeDNFCondition(er.sctx, eqFunctions...)
		if not {
			var err error
			function, err = er.newFunction(ast.UnaryNot, tp, function)
			if err != nil {
				er.err = err
				return
			}
		}
	}
	er.ctxStackPop(lLen + 1)
	er.ctxStackAppend(function, types.EmptyName)
}

func (er *expressionRewriter) caseToExpression(v *ast.CaseExpr) {
	stkLen := len(er.ctxStack)
	argsLen := 2 * len(v.WhenClauses)
	if v.ElseClause != nil {
		argsLen++
	}
	er.err = expression.CheckArgsNotMultiColumnRow(er.ctxStack[stkLen-argsLen:]...)
	if er.err != nil {
		return
	}

	// value                          -> ctxStack[stkLen-argsLen-1]
	// when clause(condition, result) -> ctxStack[stkLen-argsLen:stkLen-1];
	// else clause                    -> ctxStack[stkLen-1]
	var args []expression.Expression
	if v.Value != nil {
		// args:  eq scalar func(args: value, condition1), result1,
		//        eq scalar func(args: value, condition2), result2,
		//        ...
		//        else clause
		value := er.ctxStack[stkLen-argsLen-1]
		args = make([]expression.Expression, 0, argsLen)
		for i := stkLen - argsLen; i < stkLen-1; i += 2 {
			arg, err := er.newFunction(ast.EQ, types.NewFieldType(mysql.TypeTiny), value, er.ctxStack[i])
			if err != nil {
				er.err = err
				return
			}
			args = append(args, arg)
			args = append(args, er.ctxStack[i+1])
		}
		if v.ElseClause != nil {
			args = append(args, er.ctxStack[stkLen-1])
		}
		argsLen++ // for trimming the value element later
	} else {
		// args:  condition1, result1,
		//        condition2, result2,
		//        ...
		//        else clause
		args = er.ctxStack[stkLen-argsLen:]
	}
	function, err := er.newFunction(ast.Case, &v.Type, args...)
	if err != nil {
		er.err = err
		return
	}
	er.ctxStackPop(argsLen)
	er.ctxStackAppend(function, types.EmptyName)
}

func (er *expressionRewriter) patternLikeToExpression(v *ast.PatternLikeExpr) {
	l := len(er.ctxStack)
	er.err = expression.CheckArgsNotMultiColumnRow(er.ctxStack[l-2:]...)
	if er.err != nil {
		return
	}

	char, col := er.sctx.GetSessionVars().GetCharsetInfo()
	var function expression.Expression
	fieldType := &types.FieldType{}
	isPatternExactMatch := false
	// Treat predicate 'like' the same way as predicate '=' when it is an exact match and new collation is not enabled.
	if patExpression, ok := er.ctxStack[l-1].(*expression.Constant); ok && !collate.NewCollationEnabled() {
		patString, isNull, err := patExpression.EvalString(nil, chunk.Row{})
		if err != nil {
			er.err = err
			return
		}
		if !isNull {
			patValue, patTypes := stringutil.CompilePattern(patString, v.Escape)
			if stringutil.IsExactMatch(patTypes) && er.ctxStack[l-2].GetType().EvalType() == types.ETString {
				op := ast.EQ
				if v.Not {
					op = ast.NE
				}
				types.DefaultTypeForValue(string(patValue), fieldType, char, col)
				function, er.err = er.constructBinaryOpFunction(er.ctxStack[l-2],
					&expression.Constant{Value: types.NewStringDatum(string(patValue)), RetType: fieldType},
					op)
				isPatternExactMatch = true
			}
		}
	}
	if !isPatternExactMatch {
		types.DefaultTypeForValue(int(v.Escape), fieldType, char, col)
		function = er.notToExpression(v.Not, ast.Like, &v.Type,
			er.ctxStack[l-2], er.ctxStack[l-1], &expression.Constant{Value: types.NewIntDatum(int64(v.Escape)), RetType: fieldType})
	}

	er.ctxStackPop(2)
	er.ctxStackAppend(function, types.EmptyName)
}

func (er *expressionRewriter) regexpToScalarFunc(v *ast.PatternRegexpExpr) {
	l := len(er.ctxStack)
	er.err = expression.CheckArgsNotMultiColumnRow(er.ctxStack[l-2:]...)
	if er.err != nil {
		return
	}
	function := er.notToExpression(v.Not, ast.Regexp, &v.Type, er.ctxStack[l-2], er.ctxStack[l-1])
	er.ctxStackPop(2)
	er.ctxStackAppend(function, types.EmptyName)
}

func (er *expressionRewriter) rowToScalarFunc(v *ast.RowExpr) {
	stkLen := len(er.ctxStack)
	length := len(v.Values)
	rows := make([]expression.Expression, 0, length)
	for i := stkLen - length; i < stkLen; i++ {
		rows = append(rows, er.ctxStack[i])
	}
	er.ctxStackPop(length)
	function, err := er.newFunction(ast.RowFunc, rows[0].GetType(), rows...)
	if err != nil {
		er.err = err
		return
	}
	er.ctxStackAppend(function, types.EmptyName)
}

func (er *expressionRewriter) wrapExpWithCast() (expr, lexp, rexp expression.Expression) {
	stkLen := len(er.ctxStack)
	expr, lexp, rexp = er.ctxStack[stkLen-3], er.ctxStack[stkLen-2], er.ctxStack[stkLen-1]
	var castFunc func(sessionctx.Context, expression.Expression) expression.Expression
	switch expression.ResolveType4Between([3]expression.Expression{expr, lexp, rexp}) {
	case types.ETInt:
		castFunc = expression.WrapWithCastAsInt
	case types.ETReal:
		castFunc = expression.WrapWithCastAsReal
	case types.ETDecimal:
		castFunc = expression.WrapWithCastAsDecimal
	case types.ETString:
		castFunc = func(ctx sessionctx.Context, e expression.Expression) expression.Expression {
			// string kind expression do not need cast
			if e.GetType().EvalType().IsStringKind() {
				return e
			}
			return expression.WrapWithCastAsString(ctx, e)
		}
	case types.ETDuration:
		expr = expression.WrapWithCastAsTime(er.sctx, expr, types.NewFieldType(mysql.TypeDuration))
		lexp = expression.WrapWithCastAsTime(er.sctx, lexp, types.NewFieldType(mysql.TypeDuration))
		rexp = expression.WrapWithCastAsTime(er.sctx, rexp, types.NewFieldType(mysql.TypeDuration))
		return
	case types.ETDatetime:
		expr = expression.WrapWithCastAsTime(er.sctx, expr, types.NewFieldType(mysql.TypeDatetime))
		lexp = expression.WrapWithCastAsTime(er.sctx, lexp, types.NewFieldType(mysql.TypeDatetime))
		rexp = expression.WrapWithCastAsTime(er.sctx, rexp, types.NewFieldType(mysql.TypeDatetime))
		return
	default:
		return
	}

	expr = castFunc(er.sctx, expr)
	lexp = castFunc(er.sctx, lexp)
	rexp = castFunc(er.sctx, rexp)
	return
}

func (er *expressionRewriter) betweenToExpression(v *ast.BetweenExpr) {
	stkLen := len(er.ctxStack)
	er.err = expression.CheckArgsNotMultiColumnRow(er.ctxStack[stkLen-3:]...)
	if er.err != nil {
		return
	}

	expr, lexp, rexp := er.wrapExpWithCast()

	er.err = expression.CheckIllegalMixCollation("between", []expression.Expression{expr, lexp, rexp}, types.ETInt)
	if er.err != nil {
		return
	}

	dstCharset, dstCollation := expression.DeriveCollationFromExprs(er.sctx, expr, lexp, rexp)

	var l, r expression.Expression
	l, er.err = expression.NewFunctionBase(er.sctx, ast.GE, &v.Type, expr, lexp)
	if er.err != nil {
		return
	}
	r, er.err = expression.NewFunctionBase(er.sctx, ast.LE, &v.Type, expr, rexp)
	if er.err != nil {
		return
	}
	l.SetCharsetAndCollation(dstCharset, dstCollation)
	r.SetCharsetAndCollation(dstCharset, dstCollation)
	l = expression.FoldConstant(l)
	r = expression.FoldConstant(r)
	function, err := er.newFunction(ast.LogicAnd, &v.Type, l, r)
	if err != nil {
		er.err = err
		return
	}
	if v.Not {
		function, err = er.newFunction(ast.UnaryNot, &v.Type, function)
		if err != nil {
			er.err = err
			return
		}
	}
	er.ctxStackPop(3)
	er.ctxStackAppend(function, types.EmptyName)
}

// rewriteFuncCall handles a FuncCallExpr and generates a customized function.
// It should return true if for the given FuncCallExpr a rewrite is performed so that original behavior is skipped.
// Otherwise it should return false to indicate (the caller) that original behavior needs to be performed.
func (er *expressionRewriter) rewriteFuncCall(v *ast.FuncCallExpr) bool {
	switch v.FnName.L {
	// when column is not null, ifnull on such column is not necessary.
	case ast.Ifnull:
		if len(v.Args) != 2 {
			er.err = tidb.ErrIncorrectParameterCount.GenWithStackByArgs(v.FnName.O)
			return true
		}
		stackLen := len(er.ctxStack)
		arg1 := er.ctxStack[stackLen-2]
		col, isColumn := arg1.(*expression.Column)
		var isEnumSet bool
		if arg1.GetType().Tp == mysql.TypeEnum || arg1.GetType().Tp == mysql.TypeSet {
			isEnumSet = true
		}
		// if expr1 is a column and column has not null flag, then we can eliminate ifnull on
		// this column.
		if isColumn && !isEnumSet && mysql.HasNotNullFlag(col.RetType.Flag) {
			name := er.ctxNameStk[stackLen-2]
			newCol := col.Clone().(*expression.Column)
			er.ctxStackPop(len(v.Args))
			er.ctxStackAppend(newCol, name)
			return true
		}

		return false
	case ast.Nullif:
		if len(v.Args) != 2 {
			er.err = tidb.ErrIncorrectParameterCount.GenWithStackByArgs(v.FnName.O)
			return true
		}
		stackLen := len(er.ctxStack)
		param1 := er.ctxStack[stackLen-2]
		param2 := er.ctxStack[stackLen-1]
		// param1 = param2
		funcCompare, err := er.constructBinaryOpFunction(param1, param2, ast.EQ)
		if err != nil {
			er.err = err
			return true
		}
		// NULL
		nullTp := types.NewFieldType(mysql.TypeNull)
		nullTp.Flen, nullTp.Decimal = mysql.GetDefaultFieldLengthAndDecimal(mysql.TypeNull)
		paramNull := &expression.Constant{
			Value:   types.NewDatum(nil),
			RetType: nullTp,
		}
		// if(param1 = param2, NULL, param1)
		funcIf, err := er.newFunction(ast.If, &v.Type, funcCompare, paramNull, param1)
		if err != nil {
			er.err = err
			return true
		}
		er.ctxStackPop(len(v.Args))
		er.ctxStackAppend(funcIf, types.EmptyName)
		return true
	default:
		return false
	}
}

func (er *expressionRewriter) funcCallToExpression(v *ast.FuncCallExpr) {
	stackLen := len(er.ctxStack)
	args := er.ctxStack[stackLen-len(v.Args):]
	er.err = expression.CheckArgsNotMultiColumnRow(args...)
	if er.err != nil {
		return
	}

	if er.rewriteFuncCall(v) {
		return
	}

	var function expression.Expression
	er.ctxStackPop(len(v.Args))
	if _, ok := expression.DeferredFunctions[v.FnName.L]; er.useCache() && ok {
		// When the expression is unix_timestamp and the number of argument is not zero,
		// we deal with it as normal expression.
		if v.FnName.L == ast.UnixTimestamp && len(v.Args) != 0 {
			function, er.err = er.newFunction(v.FnName.L, &v.Type, args...)
			er.ctxStackAppend(function, types.EmptyName)
		} else {
			function, er.err = expression.NewFunctionBase(er.sctx, v.FnName.L, &v.Type, args...)
			c := &expression.Constant{Value: types.NewDatum(nil), RetType: function.GetType().Clone(), DeferredExpr: function}
			er.ctxStackAppend(c, types.EmptyName)
		}
	} else {
		function, er.err = er.newFunction(v.FnName.L, &v.Type, args...)
		er.ctxStackAppend(function, types.EmptyName)
	}
}

// Now TableName in expression only used by sequence function like nextval(seq).
// The function arg should be evaluated as a table name rather than normal column name like mysql does.
func (er *expressionRewriter) toTable(v *ast.TableName) {
	fullName := v.Name.L
	if len(v.Schema.L) != 0 {
		fullName = v.Schema.L + "." + fullName
	}
	val := &expression.Constant{
		Value:   types.NewDatum(fullName),
		RetType: types.NewFieldType(mysql.TypeString),
	}
	er.ctxStackAppend(val, types.EmptyName)
}

func (er *expressionRewriter) toColumn(v *ast.ColumnName) {
	idx, err := expression.FindFieldName(er.names, v)
	if err != nil {
		er.err = tidb.ErrAmbiguous.GenWithStackByArgs(v.Name, clauseMsg[fieldList])
		return
	}
	if idx >= 0 {
		column := er.schema.Columns[idx]
		if column.IsHidden {
			er.err = tidb.ErrUnknownColumn.GenWithStackByArgs(v.Name, clauseMsg[er.b.curClause])
			return
		}
		er.ctxStackAppend(column, er.names[idx])
		return
	}
	for i := len(er.b.outerSchemas) - 1; i >= 0; i-- {
		outerSchema, outerName := er.b.outerSchemas[i], er.b.outerNames[i]
		idx, err = expression.FindFieldName(outerName, v)
		if idx >= 0 {
			column := outerSchema.Columns[idx]
			er.ctxStackAppend(&expression.CorrelatedColumn{Column: *column, Data: new(types.Datum)}, outerName[idx])
			return
		}
		if err != nil {
			er.err = tidb.ErrAmbiguous.GenWithStackByArgs(v.Name, clauseMsg[fieldList])
			return
		}
	}
	if _, ok := er.p.(*LogicalUnionAll); ok && v.Table.O != "" {
		er.err = tidb.ErrTablenameNotAllowedHere.GenWithStackByArgs(v.Table.O, "SELECT", clauseMsg[er.b.curClause])
		return
	}
	col, name, err := findFieldNameFromNaturalUsingJoin(er.p, v)
	if err != nil {
		er.err = err
		return
	} else if col != nil {
		er.ctxStackAppend(col, name)
		return
	}
	if er.b.curClause == globalOrderByClause {
		er.b.curClause = orderByClause
	}
	er.err = tidb.ErrUnknownColumn.GenWithStackByArgs(v.String(), clauseMsg[er.b.curClause])
}

func findFieldNameFromNaturalUsingJoin(p LogicalPlan, v *ast.ColumnName) (col *expression.Column, name *types.FieldName, err error) {
	switch x := p.(type) {
	case *LogicalLimit, *LogicalSelection, *LogicalTopN, *LogicalSort:
		return findFieldNameFromNaturalUsingJoin(p.Children()[0], v)
	case *LogicalJoin:
		if x.redundantSchema != nil {
			idx, err := expression.FindFieldName(x.redundantNames, v)
			if err != nil {
				return nil, nil, err
			}
			if idx >= 0 {
				return x.redundantSchema.Columns[idx], x.redundantNames[idx], nil
			}
		}
	}
	return nil, nil, nil
}
