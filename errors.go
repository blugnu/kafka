package kafka

import (
	"errors"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// Error is a sentinel error type
type Error string

// Error implements the error interface for an Error
func (e Error) Error() string { return string(e) }

const (
	ErrHandlerFunctionIsRequired = Error("handler function is required")
	ErrInvalidReadTimeout        = Error("invalid read timeout (must be -1 or >= 0)")
	ErrNoHandler                 = Error("no handler")
	ErrReprocessMessage          = Error("message will be reprocessed")
	ErrRetryLimitReached         = Error("retry limit reached")
	ErrTimeout                   = Error("time out")
)

// ConfigurationError is an error that indicates a configuration error
type ConfigurationError struct {
	error
}

// Error implements the error interface for a ConfigurationError
func (e ConfigurationError) Error() string { return fmt.Sprintf("configuration error: %s", e.error) }

// Is determines whether the error matches some target.  The target
// is a match if it is a ConfigurationError and:
//
// - target.error is nil, or
// - target.error matches the wrapped error
func (e ConfigurationError) Is(target error) bool {
	if target, ok := target.(ConfigurationError); ok {
		return target.error == nil || errors.Is(e.error, target.error)
	}
	return false
}

// Unwrap returns the wrapped error
func (e ConfigurationError) Unwrap() error { return e.error }

// FatalError is an error that should terminate the consumer
// (irrespective of the configured error handler behaviour)
type FatalError struct{ error }

// Error implements the error interface for a FatalError
func (e FatalError) Error() string { return fmt.Sprintf("fatal error: %s", e.error) }

// Is determines whether the error matches some target.  The target
// is a match if it is a FatalError and:
//
//   - wraps a nil error, or
//   - wraps an error that matches the wrapped error
func (e FatalError) Is(target error) bool {
	if target, ok := target.(FatalError); ok {
		return target.error == nil || errors.Is(e.error, target.error)
	}
	return false
}

// Unwrap returns the wrapped error
func (e FatalError) Unwrap() error { return e.error }

// UnexpectedDeliveryEvent is an error returned when a delivery event
// was not of an expected type (Message or Event)
type UnexpectedDeliveryEvent struct {
	event kafka.Event
}

// Error implements the error interface for an UnexpectedDeliveryEvent
func (err UnexpectedDeliveryEvent) Error() string {
	return fmt.Sprintf("unexpected delivery event: %v", err.event)
}

// Is determines whether the error matches some target.  The target
// is a match if it is an UnexpectedDeliveryEvent and:
//
//   - has a nil event, or
//   - has the same event
func (err UnexpectedDeliveryEvent) Is(target error) bool {
	if target, ok := target.(UnexpectedDeliveryEvent); ok {
		return target.event == nil || target.event == err.event
	}
	return false
}
