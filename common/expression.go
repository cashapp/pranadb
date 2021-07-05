package common

import (
	"context"
	"fmt"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/owner"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/kvcache"
	"github.com/pingcap/tidb/util/sli"
	"github.com/pingcap/tipb/go-binlog"
	"github.com/squareup/pranadb/sessctx"
)

type Expression struct {
	expression expression.Expression
}

func NewColumnExpression(colIndex int, colType ColumnType) (*Expression, error) {
	tiDBType, err := ConvertPranaTypeToTiDBType(colType)
	if err != nil {
		return nil, err
	}
	col := &expression.Column{RetType: tiDBType, Index: colIndex}
	return &Expression{expression: col}, nil
}

func NewConstantInt(colType ColumnType, val int64) (*Expression, error) {
	tiDBType, err := ConvertPranaTypeToTiDBType(colType)
	if err != nil {
		return nil, err
	}
	datum := types.Datum{}
	datum.SetInt64(val)
	con := &expression.Constant{
		Value:   datum,
		RetType: tiDBType,
	}
	return &Expression{expression: con}, nil
}

func NewConstantDouble(colType ColumnType, val float64) (*Expression, error) {
	tiDBType, err := ConvertPranaTypeToTiDBType(colType)
	if err != nil {
		return nil, err
	}
	datum := types.Datum{}
	datum.SetFloat64(val)
	con := &expression.Constant{
		Value:   datum,
		RetType: tiDBType,
	}
	return &Expression{expression: con}, nil
}

func NewConstantVarchar(colType ColumnType, val string) (*Expression, error) {
	tiDBType, err := ConvertPranaTypeToTiDBType(colType)
	if err != nil {
		return nil, err
	}
	datum := types.Datum{}
	// This is the default collation for UTF-8, not sure it matters for our usage
	datum.SetString(val, "utf8mb4_0900_ai_ci")
	con := &expression.Constant{
		Value:   datum,
		RetType: tiDBType,
	}
	return &Expression{expression: con}, nil
}

func NewScalarFunctionExpression(colType ColumnType, funcName string, args ...*Expression) (*Expression, error) {
	tiDBType, err := ConvertPranaTypeToTiDBType(colType)
	if err != nil {
		return nil, err
	}
	tiDBArgs := make([]expression.Expression, len(args))
	for i, ex := range args {
		tiDBArgs[i] = ex.expression
	}
	ctx := sessctx.NewDummySessionContext()
	f, err := expression.NewFunction(ctx, funcName, tiDBType, tiDBArgs...)
	if err != nil {
		return nil, err
	}
	return &Expression{expression: f}, nil
}

func NewExpression(expression expression.Expression) *Expression {
	return &Expression{expression: expression}
}

func (e *Expression) GetColumnIndex() (int, bool) {
	exp, ok := e.expression.(*expression.Column)
	if ok {
		return exp.Index, true
	}
	return -1, false
}

func (e *Expression) EvalBoolean(row *Row) (bool, bool, error) {
	val, null, err := e.expression.EvalInt(nil, *row.tRow)
	return val != 0, null, err
}

func (e *Expression) EvalInt64(row *Row) (val int64, null bool, err error) {
	return e.expression.EvalInt(nil, *row.tRow)
}

func (e *Expression) EvalFloat64(row *Row) (val float64, null bool, err error) {
	return e.expression.EvalReal(nil, *row.tRow)
}

func (e *Expression) EvalDecimal(row *Row) (Decimal, bool, error) {
	dec, null, err := e.expression.EvalDecimal(nil, *row.tRow)
	if err != nil {
		return Decimal{}, false, err
	}
	if null {
		return Decimal{}, true, err
	}
	return *NewDecimal(dec), false, err
}

func (e *Expression) EvalString(row *Row) (val string, null bool, err error) {
	return e.expression.EvalString(nil, *row.tRow)
}

type dummySessionCtx struct {
}

func (d dummySessionCtx) NewTxn(ctx context.Context) error {
	panic("implement me")
}

func (d dummySessionCtx) NewStaleTxnWithStartTS(ctx context.Context, startTS uint64) error {
	panic("implement me")
}

func (d dummySessionCtx) Txn(active bool) (kv.Transaction, error) {
	panic("implement me")
}

func (d dummySessionCtx) GetClient() kv.Client {
	panic("implement me")
}

func (d dummySessionCtx) GetMPPClient() kv.MPPClient {
	panic("implement me")
}

func (d dummySessionCtx) SetValue(key fmt.Stringer, value interface{}) {
	panic("implement me")
}

func (d dummySessionCtx) Value(key fmt.Stringer) interface{} {
	panic("implement me")
}

func (d dummySessionCtx) ClearValue(key fmt.Stringer) {
	panic("implement me")
}

func (d dummySessionCtx) GetInfoSchema() sessionctx.InfoschemaMetaVersion {
	panic("implement me")
}

func (d dummySessionCtx) GetSessionVars() *variable.SessionVars {
	panic("implement me")
}

func (d dummySessionCtx) GetSessionManager() util.SessionManager {
	panic("implement me")
}

func (d dummySessionCtx) RefreshTxnCtx(ctx context.Context) error {
	panic("implement me")
}

func (d dummySessionCtx) RefreshVars(ctx context.Context) error {
	panic("implement me")
}

func (d dummySessionCtx) InitTxnWithStartTS(startTS uint64) error {
	panic("implement me")
}

func (d dummySessionCtx) GetStore() kv.Storage {
	panic("implement me")
}

func (d dummySessionCtx) PreparedPlanCache() *kvcache.SimpleLRUCache {
	panic("implement me")
}

func (d dummySessionCtx) StoreQueryFeedback(feedback interface{}) {
	panic("implement me")
}

func (d dummySessionCtx) HasDirtyContent(tid int64) bool {
	panic("implement me")
}

func (d dummySessionCtx) StmtCommit() {
	panic("implement me")
}

func (d dummySessionCtx) StmtRollback() {
	panic("implement me")
}

func (d dummySessionCtx) StmtGetMutation(i int64) *binlog.TableMutation {
	panic("implement me")
}

func (d dummySessionCtx) DDLOwnerChecker() owner.DDLOwnerChecker {
	panic("implement me")
}

func (d dummySessionCtx) AddTableLock(infos []model.TableLockTpInfo) {
	panic("implement me")
}

func (d dummySessionCtx) ReleaseTableLocks(locks []model.TableLockTpInfo) {
	panic("implement me")
}

func (d dummySessionCtx) ReleaseTableLockByTableIDs(tableIDs []int64) {
	panic("implement me")
}

func (d dummySessionCtx) CheckTableLocked(tblID int64) (bool, model.TableLockType) {
	panic("implement me")
}

func (d dummySessionCtx) GetAllTableLocks() []model.TableLockTpInfo {
	panic("implement me")
}

func (d dummySessionCtx) ReleaseAllTableLocks() {
	panic("implement me")
}

func (d dummySessionCtx) HasLockedTables() bool {
	panic("implement me")
}

func (d dummySessionCtx) PrepareTSFuture(ctx context.Context) {
	panic("implement me")
}

func (d dummySessionCtx) StoreIndexUsage(tblID int64, idxID int64, rowsSelected int64) {
	panic("implement me")
}

func (d dummySessionCtx) GetTxnWriteThroughputSLI() *sli.TxnWriteThroughputSLI {
	panic("implement me")
}
