package kafka_test

import (
	"errors"
	"testing"

	"github.com/blugnu/kafka"
	"github.com/blugnu/test"
	confluent "github.com/confluentinc/confluent-kafka-go/v2/kafka"
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
				msg, err := kafka.CreateMessage()

				// ASSERT
				test.That(t, err).IsNil()
				test.That(t, msg).Equals(confluent.Message{
					TopicPartition: confluent.TopicPartition{
						Partition: confluent.PartitionAny,
					},
				})
			},
		},
		{scenario: "with options",
			exec: func(t *testing.T) {
				// ARRANGE
				opterr := errors.New("option error")
				opt := func(*confluent.Message) error {
					return opterr
				}

				// ACT
				result, err := kafka.CreateMessage(opt)

				// ASSERT
				test.Error(t, err).Is(opterr)
				test.That(t, result).Equals(confluent.Message{})
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
				msg := kafka.NewMessage("topic")

				// ASSERT
				test.That(t, msg).Equals(confluent.Message{
					TopicPartition: confluent.TopicPartition{
						Topic:     test.AddressOf("topic"),
						Partition: confluent.PartitionAny,
					},
				})
			},
		},
		{scenario: "with options",
			exec: func(t *testing.T) {
				// ARRANGE
				opterr := errors.New("option error")
				opt := func(*confluent.Message) error {
					return opterr
				}
				defer test.ExpectPanic(opterr).Assert(t)

				// ACT
				result := kafka.NewMessage("topic", opt)

				// ASSERT
				test.That(t, result).Equals(confluent.Message{})
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
				msg := &confluent.Message{
					Value: []byte(`{"ID":123,"Name":"John"}`),
				}

				// ACT
				result, err := kafka.UnmarshalJSON[payload](msg)

				// ASSERT
				test.That(t, err).IsNil()
				test.That(t, result).Equals(&payload{ID: 123, Name: "John"})
			},
		},
		{scenario: "invalid JSON",
			exec: func(t *testing.T) {
				// ARRANGE
				msg := &confluent.Message{Value: []byte{}}

				// ACT
				result, err := kafka.UnmarshalJSON[string](msg)

				// ASSERT
				test.That(t, err.Error()).Equals("unexpected end of JSON input")
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
