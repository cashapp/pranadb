// Package errors provides an identical API to the popular github.com/pkg/errors package. It also provides several
// feature enhancements.
//
// Stack Traces
//
// github.com/pkg/errors includes a stacktrace for every wrapped call. This creates a lot of noise in applications
// since we prefer to always a wrap an error to ensure a logged error always has a stacktrace. This package attempts
// to remove stack traces when it can detect the traces have a shared caller. Typically you are left with only the
// root stack trace.
package errors

import (
	stderrors "errors" //nolint: depguard
	"fmt"
	"io"
	"runtime"

	"github.com/pkg/errors" //nolint: depguard
)

// github.com/pkg/errors api

// New returns an error with the supplied message.
// New also records the stack trace at the point it was called.
func New(message string) error {
	return newStackErr(nil, message)
}

// Errorf formats according to a format specifier and returns the string
// as a value that satisfies error.
// Errorf also records the stack trace at the point it was called.
func Errorf(format string, args ...interface{}) error {
	return newStackErr(nil, fmt.Sprintf(format, args...))
}

// Wrapf returns an error annotating err with a stack trace
// at the point Wrapf is called, and the format specifier.
// If err is nil, Wrapf returns nil.
func Wrapf(err error, format string, args ...interface{}) error {
	if err == nil {
		return nil
	}
	return newStackErr(err, fmt.Sprintf(format, args...))
}

// Wrap returns an error annotating err with a stack trace
// at the point Wrap is called, and the supplied message.
// If err is nil, Wrap returns nil.
func Wrap(err error, message string) error {
	if err == nil {
		return nil
	}
	return newStackErr(err, message)
}

// WithStack annotates err with a stack trace at the point WithStack was called.
// If err is nil, WithStack returns nil.
func WithStack(err error) error {
	if err == nil {
		return nil
	}
	return newStackErr(err, "")
}

// Cause returns the underlying cause of the error, if possible.
// An error value has a cause if it implements the following
// interface:
//
//     type causer interface {
//            Cause() error
//     }
//
// If the error does not implement Cause, the original error will
// be returned. If the error is nil, nil will be returned without further
// investigation.
func Cause(err error) error {
	for err != nil {
		cause, ok := err.(causer)
		if !ok {
			break
		}
		// all stackErr errors implement Cause, so check if this is the last error
		if cause.Cause() == nil {
			break
		}
		err = cause.Cause()
	}
	return err
}

// standard go errors api

// Is reports whether any error in err's chain matches target.
//
// The chain consists of err itself followed by the sequence of errors obtained by
// repeatedly calling Unwrap.
//
// An error is considered to match a target if it is equal to that target or if
// it implements a method Is(error) bool such that Is(target) returns true.
func Is(err, target error) bool { return stderrors.Is(err, target) }

// As finds the first error in err's chain that matches target, and if so, sets
// target to that error value and returns true.
//
// The chain consists of err itself followed by the sequence of errors obtained by
// repeatedly calling Unwrap.
//
// An error matches target if the error's concrete value is assignable to the value
// pointed to by target, or if the error has a method As(interface{}) bool such that
// As(target) returns true. In the latter case, the As method is responsible for
// setting target.
//
// As will panic if target is not a non-nil pointer to either a type that implements
// error, or to any interface type. As returns false if err is nil.
func As(err error, target interface{}) bool { return stderrors.As(err, target) }

type stackErr struct {
	cause error
	stack errors.StackTrace
	msg   string
}

func newStackErr(cause error, msg string) error {
	// make a stack trace for the error. remove 2 frames to account for this method and the public api calling method
	// (e.g Wrapf)
	stack := errors.New("").(stackTracer).StackTrace()[2:]
	return &stackErr{
		cause: cause,
		stack: stack,
		msg:   msg,
	}
}

func (e *stackErr) Error() string {
	if e.cause != nil {
		if e.msg != "" {
			return e.msg + ": " + e.cause.Error()
		}
		return e.cause.Error()
	}
	return e.msg
}

func (e *stackErr) Cause() error {
	return e.cause
}

// StackTrace returns non-nil if the error contains a unique StackTrace in the error chain.
//
// We aggressively wrap errors to ensure there is always a StackTrace when an error occurs in production. Unfortunately
// this leads to a lot of redundant StackTraces in the error report if there are multiple wraps. This method attempts
// to detect redundant StackTraces and only returns unique StackTraces. Typically an error report will only have a
// single root stack trace. If an error is sent between goroutines there will be a StackTrace per goroutine.
//
// Ideally this method would be called something different to make it clear it might filter StackTraces. However, the
// sentry library reflectively calls this method (thinking it's the pkg/errors library). We want to remove redundant
// StackTraces from sentry reports as well.
func (e *stackErr) StackTrace() errors.StackTrace {
	var cStack errors.StackTrace
	if sCause, ok := e.cause.(stackTracer); ok {
		if pCause, ok := e.cause.(*stackErr); ok {
			// need to special case stackErr so we don't recursively call this method. it can return nil even if there
			// is a stacktrace for the cause.
			cStack = pCause.stack
		} else {
			cStack = sCause.StackTrace()
		}
	}
	if cStack == nil || len(cStack) < len(e.stack) {
		return e.stack
	}
	// start at the bottom of the stack (i.e end of list) and walk up the call stack, comparing the program
	// counters against the cause. stop before the last one of the stack, since it's special
	for i := 1; i < len(e.stack); i++ {
		if cStack[len(cStack)-i] != e.stack[len(e.stack)-i] {
			return e.stack
		}
	}
	// cannot compare the program counters for the top of the stack and instead just compare the function names.
	if sameFn(cStack[len(cStack)-len(e.stack)], e.stack[0]) {
		return nil
	}
	return e.stack
}

// sameFn checks if the 2 stack frames are the same function. this explicitly does not check the line number since
// that does not work for the common error propagation idiom:
//
// if err := thing(); err != nil {
//   return errors.WithStack(err)
// }
func sameFn(f1 errors.Frame, f2 errors.Frame) bool {
	return file(f1) == file(f2) && name(f1) == name(f2)
}

// pc returns the program counter for this frame;
// multiple frames may have the same PC value.
func pc(f errors.Frame) uintptr { return uintptr(f) - 1 }

// file returns the full path to the file that contains the
// function for this Frame's pc.
func file(f errors.Frame) string {
	fn := runtime.FuncForPC(pc(f))
	if fn == nil {
		return "unknown"
	}
	file, _ := fn.FileLine(pc(f))
	return file
}

// name returns the name of this function, if known.
func name(f errors.Frame) string {
	fn := runtime.FuncForPC(pc(f))
	if fn == nil {
		return "unknown"
	}
	return fn.Name()
}

// Unwrap provides compatibility for Go 1.13 error chains.
func (e *stackErr) Unwrap() error { return e.cause }

// nolint:errcheck
func (e *stackErr) Format(s fmt.State, verb rune) {
	switch verb {
	case 'v':
		if s.Flag('+') {
			if e.cause != nil {
				fmt.Fprintf(s, "%+v", e.cause)
			}
			if e.msg != "" {
				if e.cause != nil {
					io.WriteString(s, "\n")
				}
				fmt.Fprintf(s, "%s", e.msg)
			}
			stack := e.StackTrace()
			if stack != nil {
				fmt.Fprintf(s, "%+v", stack)
			}
		} else {
			io.WriteString(s, e.Error())
		}
	case 's':
		io.WriteString(s, e.Error())
	case 'q':
		fmt.Fprintf(s, "%q", e.Error())
	}
}

type stackTracer interface {
	StackTrace() errors.StackTrace
}

type causer interface {
	Cause() error
}
