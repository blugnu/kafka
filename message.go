package kafka

import (
	"encoding/json"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

var jsonUnmarshal = json.Unmarshal

func MessageHeader(msg *Message, key string) string {
	for _, h := range msg.Headers {
		if string(h.Key) == key {
			return string(h.Value)
		}
	}
	return ""
}

func OffsetString(tp kafka.TopicPartition) string {
	return fmt.Sprintf("%s/%d:%d", *tp.Topic, tp.Partition, tp.Offset)
}

func UnmarshalJSON[T any](msg *kafka.Message) (*T, error) {
	result := new(T)
	if err := jsonUnmarshal(msg.Value, result); err != nil {
		return nil, err
	}
	return result, nil
}
