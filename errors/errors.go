package errors

import (
	"fmt"
	"sort"
	"strings"
)

type ErrorCode int

const (
	InternalError = iota
	SchemaNotInUse
	InvalidStatement
	UnknownSessionID
	InvalidConfiguration
	UnknownSource
	UnknownMaterializedView
	UnknownPreparedStatement
	SourceAlreadyExists
	MaterializedViewAlreadyExists
	SourceHasChildren
	MaterializedViewHasChildren

	UnknownBrokerName
	MissingKafkaBrokers
	MissingTopicInfo
	UnsupportedBrokerClientType
	UnknownTopicEncoding
	WrongNumberColumnSelectors
	InvalidSelector
	UnknownSourceOrMaterializedView
	UnknownIndexColumn
	IndexAlreadyExists

	UnknownPerfCommand

	ValueOutOfRange
	VarcharTooBig
)

func NewInternalError(seq int64) PranaError {
	return NewPranaErrorf(InternalError, "Internal error - sequence %d please consult server logs for details", seq)
}

func NewSchemaNotInUseError() PranaError {
	return NewPranaErrorf(SchemaNotInUse, "No schema in use")
}

func NewInvalidStatementError(msg string) PranaError {
	return NewPranaErrorf(InvalidStatement, msg)
}

func NewUnknownSessionIDError(sessionID string) PranaError {
	return NewPranaErrorf(UnknownSessionID, "Unknown session ID %s", sessionID)
}

func NewInvalidConfigurationError(msg string) PranaError {
	return NewPranaErrorf(InvalidConfiguration, "Invalid configuration: %s", msg)
}

func NewUnknownSourceError(schemaName string, sourceName string) PranaError {
	return NewPranaErrorf(UnknownSource, "Unknown source: %s.%s", schemaName, sourceName)
}

func NewUnknownIndexError(schemaName string, tableName string, indexName string) PranaError {
	return NewPranaErrorf(UnknownSource, "Unknown index: %s.%s.%s", schemaName, tableName, indexName)
}

func NewUnknownMaterializedViewError(schemaName string, mvName string) PranaError {
	return NewPranaErrorf(UnknownMaterializedView, "Unknown materialized view: %s.%s", schemaName, mvName)
}

func NewUnknownSourceOrMaterializedViewError(schemaName string, tableName string) PranaError {
	return NewPranaErrorf(UnknownSourceOrMaterializedView, "Unknown source or materialized view: %s.%s", schemaName, tableName)
}

func NewUnknownIndexColumn(schemaName string, tableName string, columnName string) PranaError {
	return NewPranaErrorf(UnknownIndexColumn, "Table %s.%s does not have a column %s", schemaName, tableName, columnName)
}

func NewUnknownPreparedStatementError(psID int64) PranaError {
	return NewPranaErrorf(UnknownPreparedStatement, "Unknown prepared statement, id: %d", psID)
}

func NewSourceAlreadyExistsError(schemaName string, sourceName string) PranaError {
	return NewPranaErrorf(SourceAlreadyExists, "Source already exists: %s.%s", schemaName, sourceName)
}

func NewIndexAlreadyExistsError(schemaName string, tableName string, indexName string) PranaError {
	return NewPranaErrorf(IndexAlreadyExists, "Index %s already exists on %s.%s", indexName, schemaName, tableName)
}

func NewMaterializedViewAlreadyExistsError(schemaName string, materializedViewName string) PranaError {
	return NewPranaErrorf(MaterializedViewAlreadyExists, "Materialized view already exists: %s.%s", schemaName, materializedViewName)
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

// FIXME - fix all these - quick fixed here after port from TiDB

func Trace(err error) error {
	return err
}

func AddStack(err error) error {
	return err
}

func ErrorEqual(err1 error, err2 error) bool {
	return err1 == err2
}
