package errors

import (
	"fmt"
	"sort"
	"strings"
)

type ErrorCode int

// User errors
const (
	InvalidStatement = iota + 1000
	SchemaNotInUse
	UnknownSource
	UnknownMaterializedView
	UnknownIndex
	UnknownTable
	SourceAlreadyExists
	MaterializedViewAlreadyExists
	IndexAlreadyExists
	SourceHasChildren
	MaterializedViewHasChildren
	InvalidParamCount
	Timeout
	DdlCancelled
	Unavailable
)

// Ingest errors
const (
	ValueOutOfRange = iota + 2000
	VarcharTooBig
)

// Miscellaneous errors
const (
	InvalidConfiguration = 3000
	UnknownPerfCommand   = 4000
	InternalError        = 5000
)

func NewInternalError(errReference string) PranaError {
	return NewPranaErrorf(InternalError, "Internal error - reference: %s please consult server logs for details", errReference)
}

func NewSchemaNotInUseError() PranaError {
	return NewPranaErrorf(SchemaNotInUse, "No schema in use")
}

func NewInvalidStatementError(msg string) PranaError {
	return NewPranaErrorf(InvalidStatement, msg)
}

func NewInvalidConfigurationError(msg string) PranaError {
	return NewPranaErrorf(InvalidConfiguration, "Invalid configuration: %s", msg)
}

func NewUnknownSourceError(schemaName string, sourceName string) PranaError {
	return NewPranaErrorf(UnknownSource, "Unknown source: %s.%s", schemaName, sourceName)
}

func NewUnknownIndexError(schemaName string, tableName string, indexName string) PranaError {
	return NewPranaErrorf(UnknownIndex, "Unknown index: %s.%s.%s", schemaName, tableName, indexName)
}

func NewUnknownMaterializedViewError(schemaName string, mvName string) PranaError {
	return NewPranaErrorf(UnknownMaterializedView, "Unknown materialized view: %s.%s", schemaName, mvName)
}

func NewUnknownTableError(schemaName string, tableName string) PranaError {
	return NewPranaErrorf(UnknownTable, "Unknown source or materialized view: %s.%s", schemaName, tableName)
}

func NewIndexAlreadyExistsError(schemaName string, tableName string, indexName string) PranaError {
	return NewPranaErrorf(IndexAlreadyExists, "Index %s already exists on %s.%s", indexName, schemaName, tableName)
}

func NewSourceHasChildrenError(schemaName string, sourceName string, childMVs []string) PranaError {
	return NewPranaErrorf(SourceHasChildren, "Cannot drop source %s.%s it has the following children %s", schemaName, sourceName, getChildString(schemaName, childMVs))
}

func NewMaterializedViewHasChildrenError(schemaName string, materializedViewName string, childMVs []string) PranaError {
	return NewPranaErrorf(MaterializedViewHasChildren, "Cannot drop materialized view %s.%s it has the following children %s", schemaName, materializedViewName, getChildString(schemaName, childMVs))
}

func NewUnknownLoadRunnerfCommandError(commandName string) PranaError {
	return NewPranaErrorf(UnknownPerfCommand, "Unknown perf runner command %s", commandName)
}

func NewValueOutOfRangeError(msg string) PranaError {
	return NewPranaErrorf(ValueOutOfRange, "Value out of range. %s", msg)
}

func getChildString(schemaName string, childMVs []string) string {
	sort.Strings(childMVs) // Need to sort to give deterministic results
	sb := strings.Builder{}
	for i, childName := range childMVs {
		sb.WriteString(fmt.Sprintf("%s.%s", schemaName, childName))
		if i != len(childMVs)-1 {
			sb.WriteString(", ")
		}
	}
	return sb.String()
}

func NewPranaErrorf(errorCode ErrorCode, msgFormat string, args ...interface{}) PranaError {
	msg := fmt.Sprintf(fmt.Sprintf("PDB%04d - %s", errorCode, msgFormat), args...)
	return PranaError{Code: errorCode, Msg: msg}
}

func NewPranaError(errorCode ErrorCode, msg string) PranaError {
	return PranaError{Code: errorCode, Msg: msg}
}

func Error(msg string) error {
	return New(msg)
}

// PranaError is any kind of error that is exposed to the user via external interfaces like the CLI
type PranaError struct {
	Code ErrorCode
	Msg  string
}

func (u PranaError) Error() string {
	return u.Msg
}

func MaybeConvertToPranaErrorf(err error, errorCode ErrorCode, msgFormat string, args ...interface{}) error {
	var perr PranaError
	if As(err, &perr) {
		return err
	}
	return NewPranaErrorf(errorCode, msgFormat, args...)
}

func Trace(err error) error {
	return err
}

func AddStack(err error) error {
	return err
}

func ErrorEqual(err1 error, err2 error) bool {
	return err1 == err2
}
