package kafka

import (
	"errors"
	"testing"

	"github.com/blugnu/test"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func TestCreateMessage(t *testing.T) {
	// ARRANGE
	testcases := []struct {
		scenario string
		exec     func(t *testing.T)
	}{
		{scenario: "no options",
			exec: func(t *testing.T) {
				// ACT
				msg, err := CreateMessage()

				// ASSERT
				test.That(t, err).IsNil()
				test.That(t, msg).Equals(kafka.Message{
					TopicPartition: kafka.TopicPartition{
						Partition: kafka.PartitionAny,
					},
				})
			},
		},
		{scenario: "with options",
			exec: func(t *testing.T) {
				// ARRANGE
				opterr := errors.New("option error")
				opt := func(msg *kafka.Message) error {
					return opterr
				}

				// ACT
				result, err := CreateMessage(opt)

				// ASSERT
				test.Error(t, err).Is(opterr)
				test.That(t, result).Equals(kafka.Message{})
			},
		},
	}
	for _, tc := range testcases {
		t.Run(tc.scenario, func(t *testing.T) {
			tc.exec(t)
		})
	}
}

func TestNewMessage(t *testing.T) {
	// ARRANGE
	testcases := []struct {
		scenario string
		exec     func(t *testing.T)
	}{
		{scenario: "no options",
			exec: func(t *testing.T) {
				// ACT
				msg := NewMessage("topic")

				// ASSERT
				test.That(t, msg).Equals(kafka.Message{
					TopicPartition: kafka.TopicPartition{
						Topic:     test.AddressOf("topic"),
						Partition: kafka.PartitionAny,
					},
				})
			},
		},
		{scenario: "with options",
			exec: func(t *testing.T) {
				// ARRANGE
				opterr := errors.New("option error")
				opt := func(msg *kafka.Message) error {
					return opterr
				}
				defer test.ExpectPanic(opterr).Assert(t)

				// ACT
				result := NewMessage("topic", opt)

				// ASSERT
				test.That(t, result).Equals(kafka.Message{})
			},
		},
	}
	for _, tc := range testcases {
		t.Run(tc.scenario, func(t *testing.T) {
			tc.exec(t)
		})
	}

}

func TestUnmarshalJSON(t *testing.T) {
	// ARRANGE
	testcases := []struct {
		scenario string
		exec     func(t *testing.T)
	}{
		{scenario: "valid JSON",
			exec: func(t *testing.T) {
				// ARRANGE
				type payload struct {
					ID   int
					Name string
				}
				msg := &kafka.Message{
					Value: []byte(`{"ID":123,"Name":"John"}`),
				}

				// ACT
				result, err := UnmarshalJSON[payload](msg)

				// ASSERT
				test.That(t, err).IsNil()
				test.That(t, result).Equals(&payload{ID: 123, Name: "John"})
			},
		},
		{scenario: "invalid JSON",
			exec: func(t *testing.T) {
				// ARRANGE
				jsonerr := errors.New("json error")
				msg := &kafka.Message{}
				defer test.Using(&jsonUnmarshal, func([]byte, any) error { return jsonerr })()

				// ACT
				result, err := UnmarshalJSON[string](msg)

				// ASSERT
				test.Error(t, err).Is(jsonerr)
				test.That(t, result).IsNil()
			},
		},
	}
	for _, tc := range testcases {
		t.Run(tc.scenario, func(t *testing.T) {
			tc.exec(t)
		})
	}
}
