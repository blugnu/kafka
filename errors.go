package kafka

import (
	"errors"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

var (
	ErrHandlerFunctionIsRequired = errors.New("handler function is required")
	ErrInvalidReadTimeout        = errors.New("invalid read timeout (must be -1 or >= 0)")
	ErrNoHandler                 = errors.New("no handler")
	ErrReprocessMessage          = errors.New("message will be reprocessed")
	ErrRetryLimitReached         = errors.New("retry limit reached")
	ErrTimeout                   = errors.New("time out")
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

// ConsumerPanic is an error that indicates a consumer panic
type ConsumerPanic struct{ Recovered any }

// Error implements the error interface for a ConsumerPanicError
func (e ConsumerPanic) Error() string { return fmt.Sprintf("consumer: panic: %v", e.Recovered) }

// Is determines whether the error matches some target.  The target
// is a match if it is a ConsumerPanicError and:
//
//   - has a nil Recovered, or
//   - has the same Recovered
func (e ConsumerPanic) Is(target error) bool {
	if target, ok := target.(ConsumerPanic); ok {
		return target.Recovered == nil || target.Recovered == e.Recovered
	}
	return false
}

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
