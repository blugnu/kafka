package kafka

import (
	"fmt"
	"time"
)

type OffsetReset string

const (
	OffsetResetEarliest = OffsetReset("earliest")
	OffsetResetLatest   = OffsetReset("latest")
	OffsetResetNone     = OffsetReset("none")
)

type ConsumerOption func(*Config, *consumer) error

// AutoOffsetReset sets the auto.offset.reset configuration option
// for a consumer.  This option determines what to do when there is
// no initial offset in Kafka or if the current offset no longer
// exists on the server (e.g. because that data has been deleted).
//
// Valid values are:
//
//	OffsetResetEarliest (earliest)  // the consumer will resume reading
//	                                // messages from the earliest available
//	                                // unread message for the consumer group
//
//	OffsetResetLatest   (latest)    // the consumer will resume reading
//	                                // messages from the latest available
//	                                // message, skipping any messages
//	                                // produced while the consumer group was
//	                                // not active
//
//	OffsetResetNone     (none)      // the consumer will throw an error if
//	                                // no offset is found for the consumer
//	                                // group
func AutoOffsetReset(opt OffsetReset) ConsumerOption {
	return func(cfg *Config, _ *consumer) error {
		return cfg.setKey("auto.offset.reset", string(opt))
	}
}

// ConsumerGroupID establishes the ID of the consumer group this
// consumer will join.
func ConsumerGroupID(id string) ConsumerOption {
	return func(cfg *Config, consumer *consumer) error {
		if err := cfg.setKey("group.id", id); err != nil {
			return err
		}
		consumer.groupId = id
		return nil
	}
}

// MessageDecryption sets a function the consumer will use to decrypt
// messages before passing them to the handler for that message's topic.
//
// For applications involving both consumers and producers the base
// configuration kafka.Cypher may be specified which provides both
// encryption and decryption functions.
func MessageDecryption(fn CypherFunc) ConsumerOption {
	return func(_ *Config, con *consumer) error {
		con.decrypt = fn
		return nil
	}
}

// ReadTimeoutNever is a special value for the ReadTimeout option
// that indicates the consumer should never time out when reading
// messages.
//
// Use this with caution; a consumer that never times out may be
// ejected from the consumer group if it does not receive a message
// within the configured session timeout.
const ReadTimeoutNever = time.Duration(-1)

// ReadTimeout sets the read timeout for the consumer.
// If not set, a default of 1s is used.
func ReadTimeout(timeout time.Duration) ConsumerOption {
	return func(cfg *Config, consumer *consumer) error {
		if timeout < 0 && timeout != ReadTimeoutNever {
			return ErrInvalidReadTimeout
		}
		consumer.readTimeout = timeout
		return nil
	}
}

// SeekTimeout sets the timeout for seek operations on the consumer.
// If not set, a default of 100ms is used.
func SeekTimeout(timeout time.Duration) ConsumerOption {
	return func(cfg *Config, consumer *consumer) error {
		if timeout <= 0 {
			return ErrInvalidSeekTimeout
		}
		consumer.seekTimeout = timeout
		return nil
	}
}

// TopicHandler registers a handler for a specific topic.
func TopicHandler[T comparable](topic T, h Handler) ConsumerOption {
	return func(_ *Config, c *consumer) error {
		c.handlers[fmt.Sprintf("%v", topic)] = h
		return nil
	}
}

// TopicHandlers registers multiple handlers for specific topics.
func TopicHandlers[T comparable](handlers map[T]Handler) ConsumerOption {
	return func(_ *Config, c *consumer) error {
		for topic, h := range handlers {
			_ = TopicHandler(topic, h)(nil, c)
		}
		return nil
	}
}
