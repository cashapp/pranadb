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

package planner

import (
	"fmt"
	"strings"

	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/format"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/tidb"
	"github.com/squareup/pranadb/tidb/infoschema"
	"github.com/squareup/pranadb/tidb/sessionctx"
	"github.com/squareup/pranadb/tidb/types"
	driver "github.com/squareup/pranadb/tidb/types/parser_driver"
)

// PreprocessOpt presents optional parameters to `Preprocess` method.
type PreprocessOpt func(*preprocessor)

// InPrepare is a PreprocessOpt that indicates preprocess is executing under prepare statement.
func InPrepare(p *preprocessor) {
	p.flag |= inPrepare
}

// Preprocess resolves table names of the node, and checks some statements validation.
// preprocessReturn used to extract the infoschema for the tableName and the timestamp from the asof clause.
func Preprocess(ctx sessionctx.Context, node ast.Node, preprocessOpt ...PreprocessOpt) error {
	v := preprocessor{ctx: ctx, tableAliasInJoin: make([]map[string]interface{}, 0), withName: make(map[string]interface{})}
	for _, optFn := range preprocessOpt {
		optFn(&v)
	}
	// PreprocessorReturn must be non-nil before preprocessing
	if v.PreprocessorReturn == nil {
		v.PreprocessorReturn = &PreprocessorReturn{}
	}
	node.Accept(&v)
	// InfoSchema must be non-nil after preprocessing
	v.ensureInfoSchema()
	return errors.Trace(v.err)
}

type preprocessorFlag uint8

const (
	// inPrepare is set when visiting in prepare statement.
	inPrepare preprocessorFlag = 1 << iota
	// parentIsJoin is set when visiting node's parent is join.
	parentIsJoin
)

// PreprocessorReturn is used to retain information obtained in the preprocessor.
type PreprocessorReturn struct {
	initedLastSnapshotTS bool
	ExplicitStaleness    bool
	SnapshotTSEvaluator  func(sessionctx.Context) (uint64, error)
	// LastSnapshotTS is the last evaluated snapshotTS if any
	// otherwise it defaults to zero
	LastSnapshotTS uint64
	InfoSchema     infoschema.InfoSchema
	TxnScope       string
}

// preprocessor is an ast.Visitor that preprocess
// ast Nodes parsed from parser.
type preprocessor struct {
	ctx    sessionctx.Context
	flag   preprocessorFlag
	stmtTp byte

	// tableAliasInJoin is a stack that keeps the table alias names for joins.
	// len(tableAliasInJoin) may bigger than 1 because the left/right child of join may be subquery that contains `JOIN`
	tableAliasInJoin []map[string]interface{}
	withName         map[string]interface{}

	// values that may be returned
	*PreprocessorReturn
	err error
}

func (p *preprocessor) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	switch node := in.(type) {
	case *ast.SelectStmt:
		p.stmtTp = TypeSelect
	case *ast.SetOprSelectList:
		p.checkSetOprSelectList(node)
	case *ast.Join:
		p.checkNonUniqTableAlias(node)
	case *ast.FuncCastExpr:
		p.checkFuncCastExpr(node)
	default:
		p.flag &= ^parentIsJoin
	}
	return in, p.err != nil
}

const (
	// TypeInvalid for unexpected types.
	TypeInvalid byte = iota
	// TypeSelect for SelectStmt.
	TypeSelect
)

func (p *preprocessor) tableByName(tn *ast.TableName) (*model.TableInfo, error) {
	currentDB := p.ctx.GetSessionVars().CurrentDB
	if tn.Schema.String() != "" {
		currentDB = tn.Schema.L
	}
	sName := model.NewCIStr(currentDB)
	tbl, err := p.ensureInfoSchema().TableByName(sName, tn.Name)
	return tbl, err
}

func (p *preprocessor) Leave(in ast.Node) (out ast.Node, ok bool) {
	switch x := in.(type) {
	case *driver.ParamMarkerExpr:
		if p.flag&inPrepare == 0 {
			p.err = parser.ErrSyntax.GenWithStack("syntax error, unexpected '?'")
			return
		}
	case *ast.TableName:
		p.handleTableName(x)
	case *ast.Join:
		if len(p.tableAliasInJoin) > 0 {
			p.tableAliasInJoin = p.tableAliasInJoin[:len(p.tableAliasInJoin)-1]
		}
	case *ast.FuncCallExpr:
		// The arguments for builtin NAME_CONST should be constants
		// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_name-const for details
		if x.FnName.L == ast.NameConst {
			if len(x.Args) != 2 {
				p.err = tidb.ErrIncorrectParameterCount.GenWithStackByArgs(x.FnName.L)
			} else {
				_, isValueExpr1 := x.Args[0].(*driver.ValueExpr)
				isValueExpr2 := false
				switch x.Args[1].(type) {
				case *driver.ValueExpr, *ast.UnaryOperationExpr:
					isValueExpr2 = true
				}

				if !isValueExpr1 || !isValueExpr2 {
					p.err = tidb.ErrWrongArguments.GenWithStackByArgs("NAME_CONST")
				}
			}
			break
		}
	}

	return in, p.err == nil
}

// checkSetOprSelectList checks union's selectList.
// refer: https://dev.mysql.com/doc/refman/5.7/en/union.html
//        https://mariadb.com/kb/en/intersect/
//        https://mariadb.com/kb/en/except/
// "To apply ORDER BY or LIMIT to an individual SELECT, place the clause inside the parentheses that enclose the SELECT."
func (p *preprocessor) checkSetOprSelectList(stmt *ast.SetOprSelectList) {
	for _, sel := range stmt.Selects[:len(stmt.Selects)-1] {
		switch s := sel.(type) {
		case *ast.SelectStmt:
			if s.IsInBraces {
				continue
			}
			if s.Limit != nil {
				p.err = tidb.ErrWrongUsage.GenWithStackByArgs("UNION", "LIMIT")
				return
			}
			if s.OrderBy != nil {
				p.err = tidb.ErrWrongUsage.GenWithStackByArgs("UNION", "ORDER BY")
				return
			}
		case *ast.SetOprSelectList:
			p.checkSetOprSelectList(s)
		}
	}
}

func (p *preprocessor) checkNonUniqTableAlias(stmt *ast.Join) {
	if p.flag&parentIsJoin == 0 {
		p.tableAliasInJoin = append(p.tableAliasInJoin, make(map[string]interface{}))
	}
	tableAliases := p.tableAliasInJoin[len(p.tableAliasInJoin)-1]
	isOracleMode := p.ctx.GetSessionVars().SQLMode&mysql.ModeOracle != 0
	if !isOracleMode {
		if err := isTableAliasDuplicate(stmt.Left, tableAliases); err != nil {
			p.err = err
			return
		}
		if err := isTableAliasDuplicate(stmt.Right, tableAliases); err != nil {
			p.err = err
			return
		}
	}
	p.flag |= parentIsJoin
}

func isTableAliasDuplicate(node ast.ResultSetNode, tableAliases map[string]interface{}) error {
	if ts, ok := node.(*ast.TableSource); ok {
		tabName := ts.AsName
		if tabName.L == "" {
			if tableNode, ok := ts.Source.(*ast.TableName); ok {
				if tableNode.Schema.L != "" {
					tabName = model.NewCIStr(fmt.Sprintf("%s.%s", tableNode.Schema.L, tableNode.Name.L))
				} else {
					tabName = tableNode.Name
				}
			}
		}
		_, exists := tableAliases[tabName.L]
		if len(tabName.L) != 0 && exists {
			return tidb.ErrNonUniqTable.GenWithStackByArgs(tabName)
		}
		tableAliases[tabName.L] = nil
	}
	return nil
}

func (p *preprocessor) handleTableName(tn *ast.TableName) {
	if tn.Schema.L == "" {
		if _, ok := p.withName[tn.Name.L]; ok {
			return
		}

		currentDB := p.ctx.GetSessionVars().CurrentDB
		tn.Schema = model.NewCIStr(currentDB)
	}

	tableInfo, err := p.tableByName(tn)
	if err != nil {
		p.err = err
		return
	}

	dbInfo, _ := p.ensureInfoSchema().SchemaByName(tn.Schema)

	tn.TableInfo = tableInfo
	tn.DBInfo = dbInfo
}

func (p *preprocessor) checkFuncCastExpr(node *ast.FuncCastExpr) {
	if node.Tp.EvalType() == types.ETDecimal {
		if node.Tp.Flen >= node.Tp.Decimal && node.Tp.Flen <= mysql.MaxDecimalWidth && node.Tp.Decimal <= mysql.MaxDecimalScale {
			// valid
			return
		}

		var buf strings.Builder
		restoreCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, &buf)
		if err := node.Expr.Restore(restoreCtx); err != nil {
			p.err = err
			return
		}
		if node.Tp.Flen < node.Tp.Decimal {
			p.err = tidb.ErrMBiggerThanD.GenWithStackByArgs(buf.String())
			return
		}
		if node.Tp.Flen > mysql.MaxDecimalWidth {
			p.err = tidb.ErrTooBigPrecision.GenWithStackByArgs(node.Tp.Flen, buf.String(), mysql.MaxDecimalWidth)
			return
		}
		if node.Tp.Decimal > mysql.MaxDecimalScale {
			p.err = tidb.ErrTooBigScale.GenWithStackByArgs(node.Tp.Decimal, buf.String(), mysql.MaxDecimalScale)
			return
		}
	}
}

// ensureInfoSchema get the infoschema from the preprecessor.
// there some situations:
//    - the stmt specifies the schema version.
//    - session variable
//    - transcation context
func (p *preprocessor) ensureInfoSchema() infoschema.InfoSchema {
	if p.InfoSchema == nil {
		p.InfoSchema = p.ctx.GetInfoSchema().(infoschema.InfoSchema)
	}
	return p.InfoSchema
}
