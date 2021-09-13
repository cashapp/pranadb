package perrors

import "fmt"
import gerrors "github.com/pkg/errors"

type ErrorCode int

const (
	InternalError = iota
	SchemaNotInUse
	InvalidStatement
	UnknownSessionID
	InvalidConfiguration
	UnknownSource
	UnknownMaterializedView

	PreparedStatementDoesNotExist
	UnknownBrokerName
	MissingKafkaBrokers
	MissingTopicInfo
	UnsupportedBrokerClientType
	UnknownTopicEncoding
	WrongNumberColumnSelectors
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

func NewUnknownMaterializedViewError(schemaName string, mvName string) PranaError {
	return NewPranaErrorf(UnknownMaterializedView, "Unknown materialized view: %s.%s", schemaName, mvName)
}

func NewPranaErrorf(errorCode ErrorCode, msgFormat string, args ...interface{}) PranaError {
	msg := fmt.Sprintf(fmt.Sprintf("PDB%04d - %s", errorCode, msgFormat), args...)
	return PranaError{Code: errorCode, Msg: msg}
}

func NewPranaError(errorCode ErrorCode, msg string) PranaError {
	return PranaError{Code: errorCode, Msg: msg}
}

// PranaError is any kind of error that is exposed to the user via external interfaces like the CLI
type PranaError struct {
	Code ErrorCode
	Msg  string
}

func (u PranaError) Error() string {
	return u.Msg
}

func MaybeAddStack(err error) error {
	_, ok := err.(PranaError)
	if !ok {
		return gerrors.WithStack(err)
	}
	return err
}
