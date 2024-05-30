package mock

import (
	"fmt"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func msginfo(msg *kafka.Message) string {
	if msg == nil {
		return "<no message>"
	}

	const none = "<none>"
	var (
		t string = none
		k string = none
		h string = none
		v string = none
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
