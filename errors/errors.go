package errors

import "fmt"
import gerrors "github.com/pkg/errors"

type ErrorCode int

const (
	PreparedStatementDoesNotExist = iota
	UnknownBrokerName
	MissingKafkaBrokers
	MissingTopicInfo
	UnsupportedBrokerClientType
	UnknownTopicEncoding
	WrongNumberColumnSelectors
)

func NewUserErrorF(errorCode ErrorCode, msgFormat string, arg ...interface{}) UserError {
	msg := fmt.Sprintf(msgFormat, arg...)
	return UserError{code: errorCode, msg: msg}
}

func NewUserError(errorCode ErrorCode, msg string) UserError {
	return UserError{code: errorCode, msg: msg}
}

type UserError struct {
	code ErrorCode
	msg  string
}

func (u UserError) Error() string {
	return u.msg
}

func MaybeAddStack(err error) error {
	_, ok := err.(UserError)
	if !ok {
		return gerrors.WithStack(err)
	}
	return err
}
