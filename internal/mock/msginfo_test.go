package mock //nolint: testpackage // testing a private function

import (
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func TestMsgInfo(t *testing.T) {
	// ARRANGE
	topic := "topic.id"

	testcases := []struct {
		name   string
		msg    *kafka.Message
		result string
	}{
		{name: "nil",
			msg:    nil,
			result: "<no message>",
		},
		{name: "empty message",
			msg:    &kafka.Message{},
			result: "topic=<none>, key=<none>, headers=<none>, value=<none>",
		},
		{name: "message with topic, key and value",
			msg: &kafka.Message{
				TopicPartition: kafka.TopicPartition{
					Topic: &topic,
				},
				Key:   []byte("key"),
				Value: []byte("value"),
			},
			result: "topic=\"topic.id\", key=\"key\", headers=<none>, value=\"value\"",
		},
		{name: "message with headers",
			msg: &kafka.Message{
				Headers: []kafka.Header{
					{Key: "key", Value: []byte("value")},
				},
			},
			result: "topic=<none>, key=<none>, headers=[{\"key\": \"value\"}], value=<none>",
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			// ACT
			got := msginfo(tc.msg)

			// ASSERT
			wanted := tc.result
			if wanted != got {
				t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
			}
		})
	}
}
