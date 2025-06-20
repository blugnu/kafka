package message

import (
	"encoding/json"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// function variables to facilitate testing
var (
	jsonMarshal = json.Marshal
)

// Copy copies the Value and Key from a source message.
//
// Headers are NOT copied.
func Copy(src *kafka.Message) func(*kafka.Message) error {
	return func(msg *kafka.Message) error {
		msg.Key = src.Key
		msg.Value = src.Value
		return nil
	}
}

// Header adds or replaces a message header.
func Header(k, v string) func(*kafka.Message) error {
	return func(msg *kafka.Message) error {
		for i, h := range msg.Headers {
			if h.Key == k {
				msg.Headers[i] = kafka.Header{Key: k, Value: []byte(v)}
				return nil
			}
		}
		msg.Headers = append(msg.Headers, kafka.Header{Key: k, Value: []byte(v)})
		return nil
	}
}

// Headers adds or replaces message headers.
func Headers(h map[string]string) func(*kafka.Message) error {
	return func(msg *kafka.Message) error {
		for k, v := range h {
			// the Header() option cannot return an error (the only error is a nil pointer
			// dereference, which is a programming error and will panic); we can ignore
			// any error returned as there won't be one
			_ = Header(k, v)(msg)
		}
		return nil
	}
}

// JSON sets the message value to a JSON representation of the given value.
func JSON(v any) func(*kafka.Message) error {
	return func(msg *kafka.Message) error {
		var err error
		if msg.Value, err = jsonMarshal(v); err != nil {
			return err
		}
		return nil
	}
}

// Key sets the message key.
func Key(k string) func(*kafka.Message) error {
	return func(msg *kafka.Message) error {
		msg.Key = []byte(k)
		return nil
	}
}

// Partition sets the message partition.
func Partition(p int32) func(*kafka.Message) error {
	return func(msg *kafka.Message) error {
		msg.TopicPartition.Partition = p
		return nil
	}
}

// String sets the message value to the specified string.
func String(v string) func(*kafka.Message) error {
	return func(msg *kafka.Message) error {
		msg.Value = []byte(v)
		return nil
	}
}

// Topic sets the message topic.
func Topic[T any](t T) func(*kafka.Message) error {
	return func(msg *kafka.Message) error {
		s := fmt.Sprintf("%v", t)
		msg.TopicPartition.Topic = &s
		return nil
	}
}

// Value sets the message value to a copy of the specified byte slice.
func Value(v []byte) func(*kafka.Message) error {
	return func(msg *kafka.Message) error {
		cpy := make([]byte, len(v))
		copy(cpy, v)
		msg.Value = cpy
		return nil
	}
}
