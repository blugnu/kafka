package mock //nolint: testpackage // testing a private function

import (
	"strconv"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func TestMsgInfo(t *testing.T) {
	// ARRANGE
	topic := "topic.id"

	testcases := []struct {
		msg    *kafka.Message
		result string
	}{
		{
			msg:    nil,
			result: "<no message>",
		},
		{
			msg:    &kafka.Message{},
			result: "topic=<none>, key=<none>, headers=<none>, value=<none>",
		},
		{
			msg: &kafka.Message{
				TopicPartition: kafka.TopicPartition{
					Topic: &topic,
				},
				Key:   []byte("key"),
				Value: []byte("value"),
			},
			result: "topic=\"topic.id\", key=\"key\", headers=<none>, value=\"value\"",
		},
		{
			msg: &kafka.Message{
				Headers: []kafka.Header{
					{Key: "key", Value: []byte("value")},
				},
			},
			result: "topic=<none>, key=<none>, headers=[{\"key\": \"value\"}], value=<none>",
		},
	}
	for tn, tc := range testcases {
		t.Run(strconv.Itoa(tn+1), func(t *testing.T) {
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
