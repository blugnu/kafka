package kafka

import (
	"encoding/json"

	"github.com/blugnu/kafka/message"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

var jsonUnmarshal = json.Unmarshal

// MessageOption is a function type that applies an option to a Message.
type MessageOption func(*Message) error

// CreateMessage creates a new message with the specified topic and options. Any error
// that occurs while creating the message is returned with a zero-value message.
//
// # Parameters
//
// The function is generic with a type parameter T that specifies the type of the topic;
// the topic type must support conversion to a string using fmt.Sprintf("%v").
//
// # Options
//
// Message options are applied in the order they are provided. Available options are:
//
//	message.Copy(msg *Message)            // copies the Key and Value from a specified message
//	message.Header(key, value string)     // adds a header to the message
//	message.Headers(map[string]string)    // adds headers to the message
//	message.Key(key []byte)               // sets the message key
//	message.Partition(partition int32)    // sets the message partition (will use kafka.AnyPartition by default)
//	message.Topic[T](topic T)             // sets the message topic
//	message.JSON(value any)               // sets the message value as a JSON representation of the given value
//	message.String(value string)          // sets the message value to the specified string
//	message.Value(value []byte)           // sets the message value to a copy of the specified byte slice
func CreateMessage(opts ...MessageOption) (Message, error) {
	msg := kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Partition: kafka.PartitionAny,
		},
	}
	for _, opt := range opts {
		if err := opt(&msg); err != nil {
			return Message{}, err
		}
	}
	return msg, nil
}

// NewMessage returns a new kafka.Message configured with a specified topic and any of
// the supported options, as required.
//
// # Errors
//
// If an error occurs while creating the message the function will panic.  If you need
// to handle the error use the CreateMessage function.
//
// # Parameters
//
// The function is generic with a type parameter T that specifies the type of the topic;
// the topic type must support conversion to a string using fmt.Sprintf("%v").
//
// # Options
//
// Message options are applied in the order they are provided. Available options are:
//
//	message.Copy(msg *Message)            // copies the Key and Value from a specified message
//	message.Header(key, value string)     // adds a header to the message
//	message.Headers(map[string]string)    // adds headers to the message
//	message.Key(key []byte)               // sets the message key
//	message.Partition(partition int32)    // sets the message partition (will use kafka.AnyPartition by default)
//	message.Topic[T](topic T)             // sets the message topic
//	message.JSON(value any)               // sets the message value as a JSON representation of the given value
//	message.String(value string)          // sets the message value to the specified string
//	message.Value(value []byte)           // sets the message value to a copy of the specified byte slice
//
// # Example
//
//	msg := NewMessage("topic",
//		message.Header("key", "value"),
//		message.JSON(model),
//	)
func NewMessage[T comparable](topic T, opts ...MessageOption) Message {
	opts = append([]MessageOption{message.Topic(topic)}, opts...)
	msg, err := CreateMessage(opts...)
	if err != nil {
		panic(err)
	}
	return msg
}

// UnmarshalJSON unmarshals the value of a kafka message into a new instance of the
// specified type. If the unmarshalling fails, the function returns an error.
func UnmarshalJSON[T any](msg *kafka.Message) (*T, error) {
	result := new(T)
	if err := jsonUnmarshal(msg.Value, result); err != nil {
		return nil, err
	}
	return result, nil
}
