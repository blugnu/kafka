package kafka

import (
	"encoding/json"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// messagebuilder is used to build a kafka message
// with a fluent interface.
type messagebuilder struct {
	topic     string
	headers   map[string][]byte
	key       []byte
	value     []byte
	partition int32
	err       error
}

// Build creates a new kafka message from the builder.
//
// If an error occurred during the build process, it is returned
// and no message is created.
func (m *messagebuilder) Build() (*kafka.Message, error) {
	if m.err != nil {
		return nil, m.err
	}

	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &m.topic,
			Partition: m.partition,
		},
		Key:   m.key,
		Value: m.value,
	}
	for k, v := range m.headers {
		msg.Headers = append(msg.Headers, kafka.Header{Key: k, Value: v})
	}

	return msg, nil
}

// Copy copies details for the builder from an existing message.  The
// properties copied are:
//
//   - Key
//   - Value
//   - Headers
//
// TopicPartition information is not copied.
func (m *messagebuilder) Copy(msg *kafka.Message) *messagebuilder {
	m.key = msg.Key
	m.value = msg.Value
	for _, h := range msg.Headers {
		m.headers[h.Key] = h.Value
	}
	return m
}

// OnPartition may be used to assign the TopicPartition.Partition
// of the new message.  The default is kafka.PartitionAny.
func (m *messagebuilder) OnPartition(partition int32) *messagebuilder {
	m.partition = partition
	return m
}

// WithHeader adds an individual header to the new message (or updates
// the header if already present).
func (m *messagebuilder) WithHeader(k, v string) *messagebuilder {
	m.headers[k] = []byte(v)
	return m
}

// WithHeaders adds multiple headers to the new message (or updates
// any already present).
func (m *messagebuilder) WithHeaders(headers map[string]string) *messagebuilder {
	for k, v := range headers {
		m.headers[k] = []byte(v)
	}
	return m
}

// WithKey sets the key of the new message.
func (m *messagebuilder) WithKey(key string) *messagebuilder {
	m.key = []byte(key)
	return m
}

// WithJsonValue sets the value of the new message to the JSON
// encoding of the provided value.
//
// If the value cannot be encoded as JSON, the error is stored in the
// builder and returned when Build() is called.
func (m *messagebuilder) WithJsonValue(value any) *messagebuilder {
	m.value, m.err = json.Marshal(value)
	return m
}

// WithStringValue sets the value of the new message to the provided
// string.
func (m *messagebuilder) WithStringValue(s string) *messagebuilder {
	m.value = []byte(s)
	return m
}

// NewMessage returns a builder that may be used to build a new kafka
// message using a fluent interface.  The topic is required and must
// be provided to the builder.
//
// The topic may be of any type supporting the Stringer interface.
//
// Example:
//
//	msg, err := NewMessageBuilder("topic").
//		WithHeader("key", "value").
//		WithJsonValue(model).
//		Build()
//	if err != nil {
//		return err
//	}
//
// The same fluent interface is used to build a message
// from a copy of an existing kafka message:
//
//	msg, err := NewMessageBuilder("topic").
//		Copy(kafkaMessage).
//		Build()
func NewMessage[T any](topic T) *messagebuilder {
	return &messagebuilder{
		topic:     fmt.Sprintf("%v", topic),
		headers:   map[string][]byte{},
		partition: kafka.PartitionAny,
	}
}
