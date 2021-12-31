package expression

import (
	"github.com/squareup/pranadb/tidb"
	"github.com/squareup/pranadb/tidb/sessionctx"
)

// handleInvalidTimeError reports error or warning depend on the context.
func handleInvalidTimeError(ctx sessionctx.Context, err error) error {
	if err == nil || !(tidb.ErrWrongValue.Equal(err) || tidb.ErrWrongValueForType.Equal(err) ||
		tidb.ErrTruncatedWrongVal.Equal(err) || tidb.ErrInvalidWeekModeFormat.Equal(err) ||
		tidb.ErrDatetimeFunctionOverflow.Equal(err)) {
		return err
	}
	sc := ctx.GetSessionVars().StmtCtx
	err = sc.HandleTruncate(err)
	return nil
}

// handleDivisionByZeroError reports error or warning depend on the context.
func handleDivisionByZeroError(ctx sessionctx.Context) error {
	sc := ctx.GetSessionVars().StmtCtx
	sc.AppendWarning(tidb.ErrDivisionByZero)
	return nil
}
