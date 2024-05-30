package kafka

import (
	"errors"
	"testing"

	"github.com/blugnu/test"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func TestMessageBuilder_Build_ReturnsMessage(t *testing.T) {
	// ARRANGE
	mb := &messagebuilder{
		topic:     "topic",
		key:       []byte("key"),
		value:     []byte("value"),
		partition: 1,
		headers: map[string][]byte{
			"key": []byte("value"),
		},
	}

	// ACT
	result, err := mb.Build()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// ASSERT
	test.That(t, result).Equals(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &mb.topic,
			Partition: 1,
		},
		Key:     []byte("key"),
		Value:   []byte("value"),
		Headers: []kafka.Header{{Key: "key", Value: []byte("value")}},
	})
}

func TestMessageBuilder_Build_ReturnsError(t *testing.T) {
	// ARRANGE
	mb := &messagebuilder{
		topic:     "topic",
		key:       []byte("key"),
		value:     []byte("value"),
		partition: 1,
		headers: map[string][]byte{
			"key": []byte("value"),
		},
		err: errors.New("builder error"),
	}

	// ACT
	result, err := mb.Build()

	// ASSERT
	test.That(t, result).IsNil()
	test.Error(t, err).Is(mb.err)
}

type topicid string

func TestNewMessage(t *testing.T) {
	// ACT
	result := NewMessage(topicid("topic.id"))

	// ASSERT
	test.That(t, result).Equals(&messagebuilder{
		topic:     "topic.id",
		headers:   map[string][]byte{},
		partition: kafka.PartitionAny,
	})
}

func TestMessageBuilder_Copy(t *testing.T) {
	// ARRANGE
	sut := &messagebuilder{
		topic:     "mb.topic",
		headers:   map[string][]byte{},
		partition: kafka.PartitionAny,
	}

	msgtopic := "msg.topic"
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &msgtopic,
			Partition: 1,
		},
		Key:     []byte("msg.key"),
		Value:   []byte("msg.value"),
		Headers: []kafka.Header{{Key: "msg.key", Value: []byte("msg.value")}},
	}

	// ACT
	_ = sut.Copy(msg)

	// ASSERT
	test.That(t, sut).Equals(&messagebuilder{
		topic:     "mb.topic",
		partition: kafka.PartitionAny,
		headers: map[string][]byte{
			"msg.key": []byte("msg.value"),
		},
		key:   []byte("msg.key"),
		value: []byte("msg.value"),
	})
}

func TestMessageBuilder(t *testing.T) {
	// ARRANGE
	type payload struct {
		Id int `json:"id"`
	}
	testcases := []struct {
		name   string
		fn     func(*messagebuilder)
		result messagebuilder
	}{
		{name: "on partition", fn: func(mb *messagebuilder) { mb.OnPartition(1) }, result: messagebuilder{partition: 1, headers: map[string][]byte{}}},
		{name: "with header", fn: func(mb *messagebuilder) { mb.WithHeader("key", "value") }, result: messagebuilder{headers: map[string][]byte{"key": []byte("value")}}},
		{name: "with headers", fn: func(mb *messagebuilder) { mb.WithHeaders(map[string]string{"key": "value"}) }, result: messagebuilder{headers: map[string][]byte{"key": []byte("value")}}},
		{name: "with key", fn: func(mb *messagebuilder) { mb.WithKey("key") }, result: messagebuilder{headers: map[string][]byte{}, key: []byte("key")}},
		{name: "with json value", fn: func(mb *messagebuilder) { mb.WithJsonValue(payload{1}) }, result: messagebuilder{headers: map[string][]byte{}, value: []byte(`{"id":1}`)}},
		{name: "with string value", fn: func(mb *messagebuilder) { mb.WithStringValue("payload") }, result: messagebuilder{headers: map[string][]byte{}, value: []byte(`payload`)}},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			// ARRANGE
			sut := messagebuilder{headers: map[string][]byte{}}

			// ACT
			tc.fn(&sut)

			// ASSERT
			test.That(t, sut).Equals(tc.result)
		})
	}
}
