package api

import (
	"github.com/alecthomas/participle/v2"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/errors"
)

func MaybeConvertError(err error) errors.PranaError {
	var perr errors.PranaError
	if errors.As(err, &perr) {
		return perr
	}
	var participleErr participle.Error
	if errors.As(err, &participleErr) {
		return errors.NewPranaErrorf(errors.InvalidStatement, participleErr.Error())
	}
	return common.LogInternalError(err)
}
