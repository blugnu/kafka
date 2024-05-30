package kafka

import (
	"errors"
	"testing"

	"github.com/blugnu/test"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func TestMessageHeader(t *testing.T) {
	// ARRANGE
	msg := &kafka.Message{
		Headers: []kafka.Header{
			{Key: "key1", Value: []byte("value1")},
		},
	}

	// ACT
	test.That(t, MessageHeader(msg, "key1")).Equals("value1")
	test.That(t, MessageHeader(msg, "key2")).Equals("")
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
