package kafka

import (
	"errors"
	"reflect"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/kafka"
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
	got, err := mb.Build()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// ASSERT
	wanted := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &mb.topic,
			Partition: 1,
		},
		Key:     []byte("key"),
		Value:   []byte("value"),
		Headers: []kafka.Header{{Key: "key", Value: []byte("value")}},
	}
	if !reflect.DeepEqual(wanted, got) {
		t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
	}
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
	t.Run("result", func(t *testing.T) {
		wanted := (*kafka.Message)(nil)
		got := result
		if wanted != got {
			t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
		}
	})

	t.Run("error", func(t *testing.T) {
		wanted := mb.err
		got := err
		if wanted != got {
			t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
		}
	})
}

type topicid string

func TestNewMessage(t *testing.T) {
	// ACT
	got := NewMessage(topicid("topic.id"))

	// ASSERT
	wanted := &messagebuilder{
		topic:     "topic.id",
		headers:   map[string][]byte{},
		partition: kafka.PartitionAny,
	}
	if !reflect.DeepEqual(wanted, got) {
		t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
	}
}

func TestMessageBuilder_Copy(t *testing.T) {
	// ARRANGE
	og := &messagebuilder{
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
	og.Copy(msg)

	// ASSERT
	wanted := &messagebuilder{
		topic:     "mb.topic",
		partition: kafka.PartitionAny,
		headers: map[string][]byte{
			"msg.key": []byte("msg.value"),
		},
		key:   []byte("msg.key"),
		value: []byte("msg.value"),
	}
	got := og
	if !reflect.DeepEqual(wanted, got) {
		t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
	}
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
			mb := &messagebuilder{headers: map[string][]byte{}}

			// ACT
			tc.fn(mb)

			// ASSERT
			wanted := tc.result
			got := *mb
			if !reflect.DeepEqual(wanted, got) {
				t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
			}
		})
	}
}
