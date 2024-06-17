package message

import (
	"errors"
	"testing"

	"github.com/blugnu/test"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func TestOptions(t *testing.T) {
	// ARRANGE
	testcases := []struct {
		scenario string
		exec     func(t *testing.T)
	}{
		{scenario: "Copy",
			exec: func(t *testing.T) {
				// ARRANGE
				msg := &kafka.Message{}
				src := &kafka.Message{
					Key:   []byte("key"),
					Value: []byte("value"),
					Headers: []kafka.Header{
						{Key: "header", Value: []byte("value")},
					},
					TopicPartition: kafka.TopicPartition{
						Topic:     test.AddressOf("topic"),
						Partition: 1,
					},
				}

				// ACT
				err := Copy(src)(msg)

				// ASSERT
				test.That(t, err).IsNil()
				test.That(t, msg).Equals(&kafka.Message{
					Key:            []byte("key"),
					Value:          []byte("value"),
					Headers:        nil,
					TopicPartition: kafka.TopicPartition{},
				})
			},
		},
		{scenario: "Header",
			exec: func(t *testing.T) {
				// ARRANGE
				msg := &kafka.Message{
					Headers: []kafka.Header{
						{Key: "existing", Value: []byte("value")},
					},
				}

				// ACT
				err := Header("existing", "replaced-value")(msg)
				test.That(t, err).IsNil()

				err = Header("new", "new-value")(msg)
				test.That(t, err).IsNil()

				// ASSERT
				test.That(t, msg).Equals(&kafka.Message{
					Headers: []kafka.Header{
						{Key: "existing", Value: []byte("replaced-value")},
						{Key: "new", Value: []byte("new-value")},
					},
				})
			},
		},
		{scenario: "Headers",
			exec: func(t *testing.T) {
				// ARRANGE
				msg := &kafka.Message{
					Headers: []kafka.Header{
						{Key: "existing", Value: []byte("value")},
					},
				}

				// ACT
				err := Headers(map[string]string{
					"existing": "replaced-value",
					"new":      "new-value",
				})(msg)

				// ASSERT
				test.That(t, err).IsNil()
				test.That(t, msg).Equals(&kafka.Message{
					Headers: []kafka.Header{
						{Key: "existing", Value: []byte("replaced-value")},
						{Key: "new", Value: []byte("new-value")},
					},
				})
			}},
		{scenario: "JSON/successful",
			exec: func(t *testing.T) {
				// ARRANGE
				msg := &kafka.Message{}
				payload := struct {
					ID   int
					Name string
				}{ID: 123, Name: "John"}

				// ACT
				err := JSON(payload)(msg)

				// ASSERT
				test.That(t, err).IsNil()
				test.That(t, msg.Value).Equals([]byte(`{"ID":123,"Name":"John"}`))
			},
		},
		{scenario: "JSON/failed",
			exec: func(t *testing.T) {
				// ARRANGE
				jsonerr := errors.New("json error")
				msg := &kafka.Message{}
				payload := func() {}
				defer test.Using(&jsonMarshal, func(any) ([]byte, error) { return nil, jsonerr })()

				// ACT
				err := JSON(payload)(msg)

				// ASSERT
				test.Error(t, err).Is(jsonerr)
			},
		},
		{scenario: "Key",
			exec: func(t *testing.T) {
				// ARRANGE
				msg := &kafka.Message{}

				// ACT
				err := Key("key")(msg)

				// ASSERT
				test.That(t, err).IsNil()
				test.That(t, msg).Equals(&kafka.Message{Key: []byte("key")})
			},
		},
		{scenario: "Partition",
			exec: func(t *testing.T) {
				// ARRANGE
				msg := &kafka.Message{}

				// ACT
				err := Partition(1)(msg)

				// ASSERT
				test.That(t, err).IsNil()
				test.That(t, msg).Equals(&kafka.Message{TopicPartition: kafka.TopicPartition{Partition: 1}})
			},
		},
		{scenario: "String",
			exec: func(t *testing.T) {
				// ARRANGE
				msg := &kafka.Message{}

				// ACT
				err := String("string")(msg)

				// ASSERT
				test.That(t, err).IsNil()
				test.That(t, msg).Equals(&kafka.Message{Value: []byte("string")})
			},
		},
		{scenario: "Topic",
			exec: func(t *testing.T) {
				// ARRANGE
				msg := &kafka.Message{}

				// ACT
				err := Topic("topic")(msg)

				// ASSERT
				test.That(t, err).IsNil()
				test.That(t, msg).Equals(&kafka.Message{TopicPartition: kafka.TopicPartition{Topic: test.AddressOf("topic")}})
			},
		},
		{scenario: "Value",
			exec: func(t *testing.T) {
				// ARRANGE
				msg := &kafka.Message{}

				// ACT
				err := Value([]byte("bytes"))(msg)

				// ASSERT
				test.That(t, err).IsNil()
				test.That(t, msg).Equals(&kafka.Message{Value: []byte("bytes")})
			},
		},
	}
	for _, tc := range testcases {
		t.Run(tc.scenario, func(t *testing.T) {
			tc.exec(t)
		})
	}
}
