package tidb

import (
	"github.com/squareup/pranadb/errors"
)

type PranaTError struct {
	pranaError errors.PranaError
}

func NewErr(msgFormat string) PranaTError {
	return PranaTError{
		pranaError: errors.NewPranaError(errors.InvalidStatement, msgFormat),
	}
}

func (p *PranaTError) GenWithStackByArgs(args ...interface{}) error {
	return errors.NewPranaErrorf(errors.InvalidStatement, p.pranaError.Msg, args...)
}

func (p *PranaTError) GenWithStack(args ...interface{}) error {
	return p.GenWithStackByArgs(args...)
}

func (p PranaTError) Error() string {
	return p.pranaError.Error()
}

func (p PranaTError) Equal(err error) bool {
	perr, ok := err.(PranaTError)
	if ok {
		return p.pranaError.Code == perr.pranaError.Code
	}
	prerr, ok := err.(errors.PranaError)
	if ok {
		return p.pranaError.Code == prerr.Code
	}
	return false
}

var (
	ErrIncorrectParameterCount      = NewErr("Incorrect parameter count in the call to native function '%-.192s'")
	ErrDivisionByZero               = NewErr("Division by 0")
	ErrDivByZero                    = NewErr("Division by 0")
	ErrRegexp                       = NewErr("Got error '%-.64s' from regexp")
	ErrOperandColumns               = NewErr("Operand should contain %d column(s)")
	ErrInvalidArgumentForLogarithm  = NewErr("Invalid argument for logarithm")
	ErrIncorrectType                = NewErr("Incorrect type for argument %s in function %s.")
	ErrFunctionNotExists            = NewErr("%s %s does not exist")
	ErrIncorrectArgs                = NewErr("Incorrect arguments to %s")
	ErrUnknownCharacterSet          = NewErr("Unknown character set: '%-.64s'")
	ErrDefaultValue                 = NewErr("invalid default value")
	ErrWarnAllowedPacketOverflowed  = NewErr("Result of %s() was larger than max_allowed_packet (%d) - truncated")
	ErrTruncatedWrongValue          = NewErr("Incorrect %-.32s value: '%-.128s' for column '%.192s' at row %d")
	ErrUnknownLocale                = NewErr("Unknown locale: '%-.64s'")
	ErrNonUniq                      = NewErr("Column '%-.192s' in %-.192s is ambiguous")
	ErrUnsupportedJSONComparison    = NewErr("comparison of JSON in the LEAST and GREATEST operators")
	ErrTableNotExists               = NewErr("Table '%-.192s.%-.192s' doesn't exist")
	ErrWrongObject                  = NewErr("'%-.192s.%-.192s' is not %s")
	ErrUnsupportedType              = NewErr("Unsupported type %T")
	ErrTablenameNotAllowedHere      = NewErr("Table '%s' from one of the %ss cannot be used in %s")
	ErrWrongUsage                   = NewErr("Incorrect usage of %s and %s")
	ErrUnknown                      = NewErr("Unknown error")
	ErrWrongArguments               = NewErr("Incorrect arguments to %s")
	ErrWrongNumberOfColumnsInSelect = NewErr("The used SELECT statements have a different number of columns")
	ErrFieldNotInGroupBy            = NewErr("Expression #%d of %s is not in GROUP BY clause and contains nonaggregated column '%s' which is not functionally dependent on columns in GROUP BY clause; this is incompatible with sql_mode=only_full_group_by")
	ErrAggregateOrderNonAggQuery    = NewErr("Expression #%d of ORDER BY contains aggregate function and applies to the result of a non-aggregated query")
	ErrFieldInOrderNotSelect        = NewErr("Expression #%d of ORDER BY clause is not in SELECT list, references column '%s' which is not in SELECT list; this is incompatible with %s")
	ErrAggregateInOrderNotSelect    = NewErr("Expression #%d of ORDER BY clause is not in SELECT list, contains aggregate function; this is incompatible with %s")
	ErrBadTable                     = NewErr("Unknown table '%-.100s'")
	ErrInvalidGroupFuncUse          = NewErr("Invalid use of group function")
	ErrIllegalReference             = NewErr("Reference '%-.64s' not supported (%s)")
	ErrWrongGroupField              = NewErr("Can't group on '%-.192s'")
	ErrDupFieldName                 = NewErr("Duplicate column name '%-.192s'")
	ErrInternal                     = NewErr("Internal : %s")
	ErrNonUniqTable                 = NewErr("Not unique table/alias: '%-.192s'")
	ErrInvalidWildCard              = NewErr("Wildcard fields without any table name appears in wrong place")
	ErrMixOfGroupFuncAndFields      = NewErr("In aggregated query without GROUP BY, expression #%d of SELECT list contains nonaggregated column '%s'; this is incompatible with sql_mode=only_full_group_by")
	ErrUnknownColumn                = NewErr("Unknown column '%-.192s' in '%-.192s'")
	ErrAmbiguous                    = NewErr("Column '%-.192s' in %-.192s is ambiguous")
	ErrTooBigPrecision              = NewErr("Too big precision %d specified for column '%-.192s'. Maximum is %d.")
	ErrColumnCantNull               = NewErr("Column '%-.192s' cannot be null")
	ErrGetDefaultFailed             = NewErr("Field '%s' get default value fail")
	ErrNoDefaultValue               = NewErr("Field '%-.192s' doesn't have a default value")
	ErrTruncatedWrongValueForField  = NewErr("Incorrect %-.32s value: '%-.128s' for column '%.192s' at row %d")
	ErrInvalidJSONText              = NewErr("Invalid JSON text: %-.192s")
	ErrInvalidJSONPath              = NewErr("Invalid JSON path expression %s.")
	ErrInvalidJSONData              = NewErr("Invalid JSON data provided to function %s: %s")
	ErrInvalidJSONPathWildcard      = NewErr("In this situation, path expressions may not contain the * and ** tokens.")
	ErrInvalidJSONContainsPathType  = NewErr("The second argument can only be either 'one' or 'all'.")
	ErrInvalidJSONPathArrayCell     = NewErr("A path expression is not a path to a cell in an array.")
	ErrJSONObjectKeyTooLong         = NewErr("PranaDB does not yet support JSON objects with the key length >= 65536")
	ErrDataTooLong                  = NewErr("Data too long for column '%s' at row %d")
	ErrTruncated                    = NewErr("Data truncated for column '%s' at row %d")
	ErrOverflow                     = NewErr("%s value is out of range in '%s'")
	ErrTooBigScale                  = NewErr("Too big scale %d specified for column '%-.192s'. Maximum is %d.")
	ErrBadNumber                    = NewErr("Bad Number")
	ErrMBiggerThanD                 = NewErr("For float(M,D), double(M,D) or decimal(M,D), M must be >= D (column '%-.192s').")
	ErrDatetimeFunctionOverflow     = NewErr("Datetime function: %-.32s field overflow")
	ErrCastAsSignedOverflow         = NewErr("Cast to signed converted positive out-of-range integer to it's negative complement")
	ErrCastNegIntAsUnsigned         = NewErr("Cast to unsigned converted negative integer to it's positive complement")
	ErrInvalidYearFormat            = NewErr("invalid year format")
	ErrInvalidYear                  = NewErr("invalid year")
	ErrTruncatedWrongVal            = NewErr("Truncated incorrect %-.64s value: '%-.128s'")
	ErrInvalidWeekModeFormat        = NewErr("invalid week mode format: '%v'")
	ErrWrongValue                   = NewErr("Truncated incorrect %-.64s value: '%-.128s'")
	ErrWrongValueForType            = NewErr("Incorrect %-.32s value: '%-.128s' for function %-.32s")
	ErrUnsupportedCollation         = NewErr("Unknown collation: '%-.64s'")
	ErrIllegalMixCollation          = NewErr("Illegal mix of collations for operation '%s'")
	ErrIllegalMix2Collation         = NewErr("Illegal mix of collations (%s,%s) and (%s,%s) for operation '%s'")
	ErrIllegalMix3Collation         = NewErr("Illegal mix of collations (%s,%s), (%s,%s), (%s,%s) for operation '%s'")
)
