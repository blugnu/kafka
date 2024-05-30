package log

import (
	"fmt"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type message struct {
	Topic     *offset           `json:"topic,omitempty"`
	Key       *string           `json:"key,omitempty"`
	Headers   map[string]string `json:"headers,omitempty"`
	Timestamp *time.Time        `json:"timestamp,omitempty"`
	Age       *string           `json:"age,omitempty"`
}

func (msg *message) String() string {
	s := ""
	if msg.Topic != nil {
		s += fmt.Sprintf("topic=%s, ", msg.Topic.String())
	}
	if msg.Key != nil {
		s += fmt.Sprintf("key=%s, ", *msg.Key)
	}
	if msg.Headers != nil {
		s += fmt.Sprintf("headers=%v, ", msg.Headers)
	}
	if msg.Timestamp != nil {
		s += fmt.Sprintf("timestamp={%s}, ", msg.Timestamp.String())
	}
	if msg.Age != nil {
		s += fmt.Sprintf("age=%s, ", *msg.Age)
	}
	return strings.TrimRight(s, ", ")
}

func Message(msg *kafka.Message) *message {
	result := &message{}

	if msg.TopicPartition.Topic != nil {
		result.Topic = Offset(&msg.TopicPartition)
	}

	if len(msg.Key) > 0 {
		key := string(msg.Key)
		result.Key = &key
	}
	if !msg.Timestamp.IsZero() {
		result.Timestamp = &msg.Timestamp
	}

	age := now().Sub(msg.Timestamp).String()
	result.Age = &age

	if len(msg.Headers) > 0 {
		result.Headers = map[string]string{}
		for _, h := range msg.Headers {
			result.Headers[string(h.Key)] = string(h.Value)
		}
	}

	return result
}
