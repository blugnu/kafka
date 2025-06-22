package mock

import (
	"fmt"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// msginfo returns a formatted string summarizing the topic, key, headers, and value of a Kafka message.
// If the input message is nil, it returns "<no message>". Fields that are absent are represented as "<none>".
func msginfo(msg *kafka.Message) string {
	if msg == nil {
		return "<no message>"
	}

	const none = "<none>"
	var (
		t = none
		k = none
		h = none
		v = none
	)
	if msg.TopicPartition.Topic != nil {
		t = fmt.Sprintf("%q", *msg.TopicPartition.Topic)
	}
	if len(msg.Key) > 0 {
		k = fmt.Sprintf("%q", string(msg.Key))
	}
	if len(msg.Headers) > 0 {
		h = "["
		for _, hdr := range msg.Headers {
			h += fmt.Sprintf(fmtheaderkv, hdr.Key, string(hdr.Value))
		}
		h = strings.TrimRight(h, ", ")
		h += "]"
	}
	if len(msg.Value) > 0 {
		v = fmt.Sprintf("%q", string(msg.Value))
	}

	return fmt.Sprintf("topic=%s, key=%s, headers=%s, value=%s", t, k, h, v)
}
