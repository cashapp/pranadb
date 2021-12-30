//
// This source code is a modified form of original source from the TiDB project, which has the following copyright header(s):
//

// Copyright 2016 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

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

package table

import (
	"fmt"
	"github.com/squareup/pranadb/tidb"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/squareup/pranadb/tidb/expression"
	"github.com/squareup/pranadb/tidb/sessionctx"
	"github.com/squareup/pranadb/tidb/sessionctx/stmtctx"
	"github.com/squareup/pranadb/tidb/types"
	"github.com/squareup/pranadb/tidb/types/json"
	"github.com/squareup/pranadb/tidb/util/hack"
	"github.com/squareup/pranadb/tidb/util/logutil"
	"github.com/squareup/pranadb/tidb/util/timeutil"
	"go.uber.org/zap"
)

// Column provides meta data describing a table column.
type Column struct {
	*model.ColumnInfo
	// If this column is a generated column, the expression will be stored here.
	GeneratedExpr ast.ExprNode
	// If this column has default expr value, this expression will be stored here.
	DefaultExpr ast.ExprNode
}

// String implements fmt.Stringer interface.
func (c *Column) String() string {
	ans := []string{c.Name.O, types.TypeToStr(c.Tp, c.Charset)}
	if mysql.HasAutoIncrementFlag(c.Flag) {
		ans = append(ans, "AUTO_INCREMENT")
	}
	if mysql.HasNotNullFlag(c.Flag) {
		ans = append(ans, "NOT NULL")
	}
	return strings.Join(ans, " ")
}

// ToInfo casts Column to model.ColumnInfo
// NOTE: DONT modify return value.
func (c *Column) ToInfo() *model.ColumnInfo {
	return c.ColumnInfo
}

// FindCol finds column in cols by name.
func FindCol(cols []*Column, name string) *Column {
	for _, col := range cols {
		if strings.EqualFold(col.Name.O, name) {
			return col
		}
	}
	return nil
}

// truncateTrailingSpaces truncates trailing spaces for CHAR[(M)] column.
// fix: https://github.com/pingcap/tidb/issues/3660
func truncateTrailingSpaces(v *types.Datum) {
	if v.Kind() == types.KindNull {
		return
	}
	b := v.GetBytes()
	length := len(b)
	for length > 0 && b[length-1] == ' ' {
		length--
	}
	b = b[:length]
	str := string(hack.String(b))
	v.SetString(str, v.Collation())
}

func handleWrongASCIIValue(ctx sessionctx.Context, col *model.ColumnInfo, casted *types.Datum, str string, i int) (types.Datum, error) {
	sc := ctx.GetSessionVars().StmtCtx
	err := tidb.ErrTruncatedWrongValueForField.GenWithStack("incorrect ascii value %x(%s) for column %s", casted.GetBytes(), str, col.Name)
	truncateVal := types.NewStringDatum(str[:i])
	err = sc.HandleTruncate(err)
	return truncateVal, err
}

func handleWrongUtf8Value(ctx sessionctx.Context, col *model.ColumnInfo, casted *types.Datum, str string, i int) (types.Datum, error) {
	sc := ctx.GetSessionVars().StmtCtx
	err := tidb.ErrTruncatedWrongValueForField.GenWithStack("incorrect utf8 value %x(%s) for column %s", casted.GetBytes(), str, col.Name)
	// Truncate to valid utf8 string.
	truncateVal := types.NewStringDatum(str[:i])
	err = sc.HandleTruncate(err)
	return truncateVal, err
}

// CastValue casts a value based on column type.
// If forceIgnoreTruncate is true, truncated errors will be ignored.
// If returnOverflow is true, don't handle overflow errors in this function.
// It's safe now and it's the same as the behavior of select statement.
// Set it to true only in FillVirtualColumnValue and UnionScanExec.Next()
// If the handle of err is changed latter, the behavior of forceIgnoreTruncate also need to change.
// TODO: change the third arg to TypeField. Not pass ColumnInfo.
func CastValue(ctx sessionctx.Context, val types.Datum, col *model.ColumnInfo, returnErr, forceIgnoreTruncate bool) (casted types.Datum, err error) {
	sc := ctx.GetSessionVars().StmtCtx
	// Set the reorg attribute for cast value functionality.
	if col.ChangeStateInfo != nil {
		origin := ctx.GetSessionVars().StmtCtx.InReorgAttribute
		ctx.GetSessionVars().StmtCtx.InReorgAttribute = true
		defer func() {
			ctx.GetSessionVars().StmtCtx.InReorgAttribute = origin
		}()
	}
	casted, err = val.ConvertTo(sc, &col.FieldType)
	// TODO: make sure all truncate errors are handled by ConvertTo.
	if returnErr && err != nil {
		return casted, err
	}
	if err != nil && tidb.ErrTruncated.Equal(err) && col.Tp != mysql.TypeSet && col.Tp != mysql.TypeEnum {
		str, err1 := val.ToString()
		if err1 != nil {
			logutil.BgLogger().Warn("Datum ToString failed", zap.Stringer("Datum", val), zap.Error(err1))
		}
		err = tidb.ErrTruncatedWrongVal.GenWithStackByArgs(col.FieldType.CompactStr(), str)
	}

	err = sc.HandleTruncate(err)

	if forceIgnoreTruncate {
		err = nil
	} else if err != nil {
		return casted, err
	}

	if col.Tp == mysql.TypeString && !types.IsBinaryStr(&col.FieldType) {
		truncateTrailingSpaces(&casted)
	}

	if col.Charset == charset.CharsetASCII {
		str := casted.GetString()
		for i := 0; i < len(str); i++ {
			if str[i] > unicode.MaxASCII {
				casted, err = handleWrongASCIIValue(ctx, col, &casted, str, i)
				break
			}
		}
		if forceIgnoreTruncate {
			err = nil
		}
		return casted, err
	}

	if !mysql.IsUTF8Charset(col.Charset) {
		return casted, nil
	}
	str := casted.GetString()
	for i, w := 0, 0; i < len(str); i += w {
		runeValue, width := utf8.DecodeRuneInString(str[i:])
		if runeValue == utf8.RuneError {
			if strings.HasPrefix(str[i:], string(utf8.RuneError)) {
				w = width
				continue
			}
			casted, err = handleWrongUtf8Value(ctx, col, &casted, str, i)
			break
		} else if width > 3 {
			// Handle non-BMP characters.
			casted, err = handleWrongUtf8Value(ctx, col, &casted, str, i)
			break
		}
		w = width
	}

	if forceIgnoreTruncate {
		err = nil
	}
	return casted, err
}

// ColDesc describes column information like MySQL desc and show columns do.
type ColDesc struct {
	Field string
	Type  string
	// Charset is nil if the column doesn't have a charset, or a string indicating the charset name.
	Charset interface{}
	// Collation is nil if the column doesn't have a collation, or a string indicating the collation name.
	Collation    interface{}
	Null         string
	Key          string
	DefaultValue interface{}
	Extra        string
	Privileges   string
	Comment      string
}

// CheckNotNull checks if nil value set to a column with NotNull flag is set.
func (c *Column) CheckNotNull(data *types.Datum) error {
	if (mysql.HasNotNullFlag(c.Flag) || mysql.HasPreventNullInsertFlag(c.Flag)) && data.IsNull() {
		return tidb.ErrColumnCantNull.GenWithStackByArgs(c.Name)
	}
	return nil
}

// HandleBadNull handles the bad null error.
// If BadNullAsWarning is true, it will append the error as a warning, else return the error.
func (c *Column) HandleBadNull(d *types.Datum, sc *stmtctx.StatementContext) error {
	if err := c.CheckNotNull(d); err != nil {
		return err
	}
	return nil
}

// IsPKHandleColumn checks if the column is primary key handle column.
func (c *Column) IsPKHandleColumn(tbInfo *model.TableInfo) bool {
	return mysql.HasPriKeyFlag(c.Flag) && tbInfo.PKIsHandle
}

// IsCommonHandleColumn checks if the column is common handle column.
func (c *Column) IsCommonHandleColumn(tbInfo *model.TableInfo) bool {
	return mysql.HasPriKeyFlag(c.Flag) && tbInfo.IsCommonHandle
}

// GetColOriginDefaultValue gets default value of the column from original default value.
func GetColOriginDefaultValue(ctx sessionctx.Context, col *model.ColumnInfo) (types.Datum, error) {
	return getColDefaultValue(ctx, col, col.GetOriginDefaultValue())
}

// GetColDefaultValue gets default value of the column.
func GetColDefaultValue(ctx sessionctx.Context, col *model.ColumnInfo) (types.Datum, error) {
	defaultValue := col.GetDefaultValue()
	if !col.DefaultIsExpr {
		return getColDefaultValue(ctx, col, defaultValue)
	}
	return getColDefaultExprValue(ctx, col, defaultValue.(string))
}

// EvalColDefaultExpr eval default expr node to explicit default value.
func EvalColDefaultExpr(ctx sessionctx.Context, col *model.ColumnInfo, defaultExpr ast.ExprNode) (types.Datum, error) {
	d, err := expression.EvalAstExpr(ctx, defaultExpr)
	if err != nil {
		return types.Datum{}, err
	}
	// Check the evaluated data type by cast.
	value, err := CastValue(ctx, d, col, false, false)
	if err != nil {
		return types.Datum{}, err
	}
	return value, nil
}

func getColDefaultExprValue(ctx sessionctx.Context, col *model.ColumnInfo, defaultValue string) (types.Datum, error) {
	var defaultExpr ast.ExprNode
	expr := fmt.Sprintf("select %s", defaultValue)
	stmts, _, err := parser.New().Parse(expr, "", "")
	if err == nil {
		defaultExpr = stmts[0].(*ast.SelectStmt).Fields.Fields[0].Expr
	}
	d, err := expression.EvalAstExpr(ctx, defaultExpr)
	if err != nil {
		return types.Datum{}, err
	}
	// Check the evaluated data type by cast.
	value, err := CastValue(ctx, d, col, false, false)
	if err != nil {
		return types.Datum{}, err
	}
	return value, nil
}

func getColDefaultValue(ctx sessionctx.Context, col *model.ColumnInfo, defaultVal interface{}) (types.Datum, error) {
	if defaultVal == nil {
		return getColDefaultValueFromNil(ctx, col)
	}

	if col.Tp != mysql.TypeTimestamp && col.Tp != mysql.TypeDatetime {
		value, err := CastValue(ctx, types.NewDatum(defaultVal), col, false, false)
		if err != nil {
			return types.Datum{}, err
		}
		return value, nil
	}

	// Check and get timestamp/datetime default value.
	sc := ctx.GetSessionVars().StmtCtx
	var needChangeTimeZone bool
	// If the column's default value is not ZeroDatetimeStr nor CurrentTimestamp, should use the time zone of the default value itself.
	if col.Tp == mysql.TypeTimestamp {
		if vv, ok := defaultVal.(string); ok && vv != types.ZeroDatetimeStr && !strings.EqualFold(vv, ast.CurrentTimestamp) {
			needChangeTimeZone = true
			originalTZ := sc.TimeZone
			// For col.Version = 0, the timezone information of default value is already lost, so use the system timezone as the default value timezone.
			sc.TimeZone = timeutil.SystemLocation()
			if col.Version >= model.ColumnInfoVersion1 {
				sc.TimeZone = time.UTC
			}
			defer func() { sc.TimeZone = originalTZ }()
		}
	}
	value, err := expression.GetTimeValue(ctx, defaultVal, col.Tp, int8(col.Decimal))
	if err != nil {
		return types.Datum{}, tidb.ErrGetDefaultFailed.GenWithStackByArgs(col.Name)
	}
	// If the column's default value is not ZeroDatetimeStr or CurrentTimestamp, convert the default value to the current session time zone.
	if needChangeTimeZone {
		t := value.GetMysqlTime()
		err = t.ConvertTimeZone(sc.TimeZone, ctx.GetSessionVars().Location())
		if err != nil {
			return value, err
		}
		value.SetMysqlTime(t)
	}
	return value, nil
}

func getColDefaultValueFromNil(ctx sessionctx.Context, col *model.ColumnInfo) (types.Datum, error) {
	if !mysql.HasNotNullFlag(col.Flag) {
		return types.Datum{}, nil
	}
	if col.Tp == mysql.TypeEnum {
		// For enum type, if no default value and not null is set,
		// the default value is the first element of the enum list
		defEnum, err := types.ParseEnumValue(col.FieldType.Elems, 1)
		if err != nil {
			return types.Datum{}, err
		}
		return types.NewCollateMysqlEnumDatum(defEnum, col.Collate), nil
	}
	if mysql.HasAutoIncrementFlag(col.Flag) {
		// Auto increment column doesn't has default value and we should not return error.
		return GetZeroValue(col), nil
	}
	vars := ctx.GetSessionVars()
	sc := vars.StmtCtx
	sc.AppendWarning(tidb.ErrNoDefaultValue.GenWithStack(col.Name))
	return GetZeroValue(col), nil
}

// GetZeroValue gets zero value for given column type.
func GetZeroValue(col *model.ColumnInfo) types.Datum {
	var d types.Datum
	switch col.Tp {
	case mysql.TypeTiny, mysql.TypeInt24, mysql.TypeShort, mysql.TypeLong, mysql.TypeLonglong:
		if mysql.HasUnsignedFlag(col.Flag) {
			d.SetUint64(0)
		} else {
			d.SetInt64(0)
		}
	case mysql.TypeYear:
		d.SetInt64(0)
	case mysql.TypeFloat:
		d.SetFloat32(0)
	case mysql.TypeDouble:
		d.SetFloat64(0)
	case mysql.TypeNewDecimal:
		d.SetLength(col.Flen)
		d.SetFrac(col.Decimal)
		d.SetMysqlDecimal(new(types.MyDecimal))
	case mysql.TypeString:
		if col.Flen > 0 && col.Charset == charset.CharsetBin {
			d.SetBytes(make([]byte, col.Flen))
		} else {
			d.SetString("", col.Collate)
		}
	case mysql.TypeVarString, mysql.TypeVarchar, mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		d.SetString("", col.Collate)
	case mysql.TypeDuration:
		d.SetMysqlDuration(types.ZeroDuration)
	case mysql.TypeDate:
		d.SetMysqlTime(types.ZeroDate)
	case mysql.TypeTimestamp:
		d.SetMysqlTime(types.ZeroTimestamp)
	case mysql.TypeDatetime:
		d.SetMysqlTime(types.ZeroDatetime)
	case mysql.TypeBit:
		d.SetMysqlBit(types.ZeroBinaryLiteral)
	case mysql.TypeSet:
		d.SetMysqlSet(types.Set{}, col.Collate)
	case mysql.TypeEnum:
		d.SetMysqlEnum(types.Enum{}, col.Collate)
	case mysql.TypeJSON:
		d.SetMysqlJSON(json.CreateBinary(nil))
	}
	return d
}
