package kafka

import (
	"context"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// Retry is an OnErrorHandler that produces retry messages to a retry topic.
//
// The RetryHandler is delegates applying and reading retry metadata to/from messages
// to a RetryMetadataHandler.
//
// A RetryThrottler is used to determine a maximum number of retries and any message
// processing intervals.
type Retry[T comparable] struct {
	Topic           T                    // the topic to which to publish the message; if undefined (zero value), retry events are produced to the original topic
	MetadataHandler RetryMetadataHandler // the RetryHandler is responsible for applying and reading retry metadata to/from messages
	Throttling      RetryThrottler       // if undefined, unlimited retry events are posted to the retry topic for immediate processing until handled successfully; otherwise, the throttler is used to determine a maximum number of retries and message processing delays
	*consumer                            // the consumer to which the retry handler is attached; this is expected to provide the Producer used for producing retry messages
}

// HandleError provides a retry mechanism for handling errors by logging the error
// and producing a retry message.
//
// If retry is not possible due to the retry limit being reached ErrRetryLimitReached is returned.
// If the producer fails to produce the retry message the producer error is returned.
func (retry *Retry[T]) HandleError(ctx context.Context, msg *kafka.Message, err error) error {
	log := Logger.WithContext(ctx)
	log.Error(err)

	// get retry metadata from the message
	md := retry.MetadataHandler.Get(msg)
	if md == nil {
		md = &RetryMetadata{Seq: 1}
	}

	// set the Reason on the retry metadata and apply throttling
	md.Reason = err.Error()
	if err := retry.Throttling.Update(md); err != nil {
		return fmt.Errorf("retry: HandleError: %w", err)
	}

	// create a copy of the message and set the retry metadata
	copy := *msg
	msg = &copy
	retry.MetadataHandler.Set(msg, md)

	// produce the retry message
	if _, err := retry.MustProduce(ctx, msg); err != nil {
		return fmt.Errorf("retry: HandleError: failed to produce retry message: %w", err)
	}

	return nil
}
