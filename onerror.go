package kafka

import (
	"context"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type OnErrorHandler interface {
	HandleError(context.Context, *kafka.Message, error) error
}

type halt struct{}

func (e *halt) HandleError(ctx context.Context, msg *kafka.Message, err error) error {
	Logger.WithContext(ctx).Error(err)
	return FatalError{err}
}

type onerror struct {
	returns error
}

func (e *onerror) HandleError(ctx context.Context, msg *kafka.Message, err error) error {
	Logger.WithContext(ctx).Error(err)
	return e.returns
}

var _halt OnErrorHandler = &halt{}
var _reprocess OnErrorHandler = &onerror{ErrReprocessMessage}
var _skip OnErrorHandler = &onerror{nil}

func HaltConsumer() OnErrorHandler     { return _halt }
func ReprocessMessage() OnErrorHandler { return _reprocess }
func SkipMessage() OnErrorHandler      { return _skip }
