package kafka

import (
	"fmt"
	"log"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"

	"context"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// LogInfo contains information about a logging event; it is used to pass
// information to logger functions configured by an application using this
// module.
//
// LogInfo implements the fmt.Stringer interface and provides a string
// representation of the log event.  However, this string representation is
// intended to provide a human-readable representation of the log event
// to support unit tests and debugging; it is not intended to be used for
// log serialization which should be implemented by the logger functions
// configured by the application.
type LogInfo struct {
	// Consumer is the consumer group that the log event is related to
	Consumer *string `json:"consumer,omitempty"`

	// Error is an error that the log event is related to
	Error error `json:"-"`

	// Recovered is a value that has been recovered from a panic
	Recovered *any `json:"-"`

	// Headers is a map of headers from a message
	Headers map[string][]byte `json:"headers,omitempty"`

	// Key is the key of a message
	Key []byte `json:"key,omitempty"`

	// Offset is the offset of a message
	Offset *kafka.Offset `json:"offset,omitempty"`

	// Partition is the partition of a message or otherwise related to the log event
	// (e.g. when logging a producer error)
	Partition *int32 `json:"partition,omitempty"`

	// Topic is the topic of a message or otherwise related to the log event
	// (e.g. when logging a producer error)
	Topic *string `json:"topic,omitempty"`

	// Topics identifies 1 or more topics when a log event relates to those topics.
	// It should not be used to identify a single topic when a log event relates to
	// a message; the Topic field should be used instead.
	Topics *[]string `json:"topics,omitempty"`

	// Timestamp is the timestamp of a message
	Timestamp *time.Time `json:"timestamp,omitempty"`

	// Reason is a reason that the log event is being logged
	Reason *string `json:"reason,omitempty"`
}

// String implements the fmt.Stringer interface and returns a string
// representation of the LogInfo.  This implementation is provided
// for convenience (primarily in unit test output) and is not intended
// to be used for log serialization.
//
// # Details
//
// The string representation is a space-separated list of key=<value> pairs
// where the key is the name of the field and the value is the string
// representation of the field's value.
//
// The string representation of the value of each field is determined by
// the type of the field:
//
//	[]byte              // converted to string, enclosed by [square brackets]
//	identifier          // string (consumer, topic fields)
//	string              // quoted string (including error field)
//	int32               // string representation of the integer
//	kafka.Offset:       // string representation of the offset
//	time.Time           // RFC3339 representation of the timestamp
//	any                 // %v representation of the value (recovered field)
//
// Message headers are represented as a map of key-value pairs enclosed
// by {curly braces} where the key is the header key and the value is the
// []byte string representation of the header value.
func (info LogInfo) String() string {
	e := []string{}
	f := info.fields()

	keys := []string{}
	for k := range f {
		keys = append(keys, k)
	}
	slices.Sort(keys)

	for _, k := range keys {
		switch k {
		case "headers":
			s := []string{}
			keys := []string{}
			for k := range info.Headers {
				keys = append(keys, k)
			}
			slices.Sort(keys)
			for _, k := range keys {
				s = append(s, fmt.Sprintf("%s:[%s]", k, info.Headers[k]))
			}
			e = append(e, fmt.Sprintf("headers={%s}", strings.Join(s, ", ")))

		case "topics":
			e = append(e, "topics=["+strings.Join(*info.Topics, ", ")+"]")

		default:
			vf := map[string]string{
				"consumer":  "%s",
				"error":     "%q",
				"key":       "%s",
				"offset":    "%s",
				"partition": "%s",
				"reason":    "%q",
				"recovered": "%q",
				"timestamp": "%s",
				"topic":     "%s",
			}[k]
			e = append(e, fmt.Sprintf("%s="+vf, k, f[k]))
		}
	}

	return strings.Join(e, " ")
}

type logfieldsBuilder struct {
	fields map[string]string
}

func (b *logfieldsBuilder) add(name string, value any) { //nolint: cyclop // type switch is straightforward
	if value == nil || reflect.ValueOf(value).IsZero() {
		return
	}

	switch v := value.(type) {
	case []byte:
		b.fields[name] = "[" + string(v) + "]"
	case error:
		b.fields[name] = v.Error()
	case *any:
		b.fields[name] = fmt.Sprintf("%v", *v)
	case *int32:
		b.fields[name] = strconv.Itoa(int(*v))
	case *kafka.Offset:
		b.fields[name] = v.String()
	case *string:
		b.fields[name] = *v
	case *time.Time:
		b.fields[name] = v.Format(time.RFC3339)
	default:
		panic(fmt.Errorf("%w: values of type %T are not supported as field values", ErrInvalidOperation, value))
	}
}

// fields returns a map of the fields in the LogInfo.  This method is used
// by unit tests when rendering the LogInfo as a string and emitting field
// values to a mocked logger.
func (info LogInfo) fields() map[string]string {
	result := logfieldsBuilder{
		fields: make(map[string]string),
	}

	result.add("consumer", info.Consumer)
	result.add("topic", info.Topic)
	result.add("partition", info.Partition)
	result.add("offset", info.Offset)
	result.add("key", info.Key)
	if len(info.Headers) > 0 {
		headers := map[string]string{}
		for k, v := range info.Headers {
			headers[k] = string(v)
		}
		result.fields["headers"] = fmt.Sprintf("%v", headers)
	}
	result.add("timestamp", info.Timestamp)
	result.add("error", info.Error)
	result.add("reason", info.Reason)
	result.add("recovered", info.Recovered)
	if info.Topics != nil {
		result.fields["topics"] = fmt.Sprintf("%v", *info.Topics)
	}

	return result.fields
}

// withMessageDetails returns a copy of the receiver with the topic,
// partition, offset, key, headers and timestamp set to those of the
// provided message.
//
// The function uses withOffsetDetails to initialize Topic, Partition
// and Offset information.  Therefore, calling this function will also
// set the Topics field to nil even if the message TopicPartition is
// zero-value.
func (info LogInfo) withMessageDetails(msg *kafka.Message) LogInfo {
	info = LogInfo.withOffsetDetails(info, msg.TopicPartition)

	info.Key = msg.Key

	// headers are transformed into a map so that the LogInfo
	// can be rendered as JSON using the standard encoding/json
	// package if desired
	if len(msg.Headers) > 0 {
		info.Headers = make(map[string][]byte)
		for _, header := range msg.Headers {
			info.Headers[header.Key] = header.Value
		}
	}

	// only set the timestamp if it is not the zero value to prevent a
	// zero value rendered if the LogInfo is marshalled to JSON
	if !msg.Timestamp.IsZero() {
		info.Timestamp = &msg.Timestamp
	}

	return info
}

// withOffsetDetails returns a copy of the receiver with the topic, partition
// and offset set to those of the provided topic partition.
//
// If the provided topic partition is a zero value, the topic, partition
// and offset fields are set to nil.
//
// Calling this method will set the Topics field to nil (even if the provided
// topic partition is a zero value).
func (info LogInfo) withOffsetDetails(tp kafka.TopicPartition) LogInfo {
	info.Topic = tp.Topic
	info.Topics = nil

	if z := (kafka.TopicPartition{}); tp == z {
		info.Partition = nil
		info.Offset = nil
	} else {
		info.Partition = &tp.Partition
		info.Offset = &tp.Offset
	}

	return info
}

// Loggers is a struct that contains functions for logging debug, info and error
// messages.  The functions are expected to log the message and the LogInfo
// provided.
type Loggers struct {
	Debug func(context.Context, string, LogInfo)
	Info  func(context.Context, string, LogInfo)
	Error func(context.Context, string, LogInfo)
}

// useDefault returns true if the Loggers is nil or if all of the debug, info
// and error functions are niinfo.
func (l *Loggers) useDefault() bool {
	return l == nil ||
		(l.Debug == nil && l.Info == nil && l.Error == nil)
}

// noLog is a no-op logging function that does nothing.
func noLog(context.Context, string, LogInfo) { /* NO-OP */ }

// logs holds the configured loggers.  By default, the loggers are set to
// noLog, which is a no-op function that does nothing.
var logs = Loggers{
	Debug: noLog,
	Info:  noLog,
	Error: noLog,
}

// EnableLogs sets the loggers to the functions provided in the supplied
// Loggers struct.  If any of the loggers are nil, the default loggers are
// used for those log levels.
//
// # Default Loggers
//
// If a nil Loggers is specified or if ALL of the loggers are nil, default
// loggers will be configured that log to the standard logger using log.Println.
// The default logger output is prefixed with the KAFKA:<LEVEL>, the log message
// and the LogInfo struct.  The LogInfo struct is rendered using the .String(), e.g.:
//
//	KAFKA:ERROR commit failed consumer=group-id topic=message-topic partition=1 offset=2 key=[key-value] headers={key1:[value1] key2:[value2]} timestamp=2010-09-08T07:06:05Z error="error"
//
// # example
//
// This example logs Info and Error logs to the standard logger. The Debug
// logger is set to a no-op function.
//
//	EnableLogs(&Loggers{
//		Info: func(ctx context.Context, s string, i LogInfo) {
//			log.Println("INFO", s, i.String())
//		},
//		Error: func(ctx context.Context, s string, i LogInfo) {
//			log.Println("ERROR", s, i.String())
//		},
//	})
//
// Note that logging the LogInfo using the .String() method is not
// recommended; loggers should use the LogInfo struct to extract the
// information required for logging and render that to the application
// log in the appropriate form.  e.g. using slog fields.
func EnableLogs(fns *Loggers) {
	type logfn func(context.Context, string, LogInfo)

	ifNotNil := func(fn logfn) logfn {
		if fn == nil {
			return noLog
		}
		return fn
	}
	makeFunc := func(name string) logfn {
		return func(ctx context.Context, s string, i LogInfo) {
			log.Println(name, s, i)
		}
	}

	if fns.useDefault() {
		logs.Debug = makeFunc("KAFKA:DEBUG")
		logs.Info = makeFunc("KAFKA:INFO ")
		logs.Error = makeFunc("KAFKA:ERROR")
		return
	}

	logs.Debug = ifNotNil(fns.Debug)
	logs.Info = ifNotNil(fns.Info)
	logs.Error = ifNotNil(fns.Error)
}
