package kafka

import (
	"errors"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

var (
	ErrConsumerError        = errors.New("consumer error")
	ErrConsumerNotRunning   = errors.New("consumer is not running")
	ErrConsumerNotStarted   = errors.New("consumer has not been started")
	ErrInvalidOperation     = errors.New("invalid operation")
	ErrInvalidReadTimeout   = errors.New("invalid read timeout (must be kafka.ReadTimeoutNever or >= 0ms)")
	ErrInvalidSeekTimeout   = errors.New("invalid seek timeout (must be > 0ms)")
	ErrNoHandler            = errors.New("no handler for topic")
	ErrNoHandlersConfigured = errors.New("no handlers configured")
	ErrReprocessMessage     = errors.New("message will be reprocessed")
	ErrRetryLimitReached    = errors.New("retry limit reached")
	ErrTimeout              = errors.New("time out")
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

// ConsumerPanicError is an error that indicates a consumer panic
type ConsumerPanicError struct {
	Recovered any
}

// Error implements the error interface for a ConsumerPanicError
func (e ConsumerPanicError) Error() string {
	return fmt.Sprintf("consumer panic: recovered: %v", e.Recovered)
}

// Is determines whether the error matches some target.  The target
// is a match if it is a ConsumerPanicError and:
//
//   - has a nil Recovered, or
//   - has a non-nil Recovered which equals the target.Recovered
func (e ConsumerPanicError) Is(target error) bool {
	if target, ok := target.(ConsumerPanicError); ok {
		return (target.Recovered == nil || target.Recovered == e.Recovered)
	}
	return false
}

// UnexpectedDeliveryEvent is an error returned when a delivery event
// was not of an expected type (Message or Event)
type UnexpectedDeliveryEvent struct { //nolint: errname // this follows the pattern of a confluent kafka event that *may* be an error (i.e. not an idiomatic Go error type)
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

// IsKafkaError checks whether the error is a kafka.Error and has the specified code.
// It returns true if the error is a kafka.Error and its code matches the provided code.
func IsKafkaError(err error, code kafka.ErrorCode) bool {
	var kerr kafka.Error
	if ok := errors.As(err, &kerr); ok {
		return kerr.Code() == code
	}
	return false
}
