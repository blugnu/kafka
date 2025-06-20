package kafka_test

import (
	"testing"

	"github.com/blugnu/kafka"
	"github.com/blugnu/test"
	confluent "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func TestHeaders(t *testing.T) {
	// ARRANGE
	testcases := []struct {
		scenario string
		exec     func(t *testing.T)
	}{
		{scenario: "Get",
			exec: func(t *testing.T) {
				// ARRANGE
				msg := &confluent.Message{
					Headers: []confluent.Header{
						{Key: "key1", Value: []byte("value1")},
						{Key: "key2", Value: []byte("value2")},
					},
				}

				// ACT
				h := kafka.Headers(msg)

				// ASSERT
				test.That(t, h.Get("key1")).Equals([]byte("value1"))
				test.That(t, h.Get("key2")).Equals([]byte("value2"))
				test.That(t, h.Get("key3")).Equals(nil)
			},
		},
		{scenario: "GetString",
			exec: func(t *testing.T) {
				// ARRANGE
				msg := &confluent.Message{
					Headers: []confluent.Header{
						{Key: "key1", Value: []byte("value1")},
						{Key: "key2", Value: []byte("value2")},
					},
				}

				// ACT
				h := kafka.Headers(msg)

				// ASSERT
				test.That(t, h.GetString("key1")).Equals("value1")
				test.That(t, h.GetString("key2")).Equals("value2")
				test.That(t, h.GetString("key3")).Equals("")
			},
		},
		{scenario: "HasKey",
			exec: func(t *testing.T) {
				// ARRANGE
				msg := &confluent.Message{
					Headers: []confluent.Header{
						{Key: "key1", Value: []byte("value1")},
						{Key: "key2", Value: []byte("value2")},
					},
				}
				h := kafka.Headers(msg)

				// ACT/ASSERT
				test.IsTrue(t, h.HasKey("key1"))
				test.IsTrue(t, h.HasKey("key2"))
				test.IsFalse(t, h.HasKey("key3"))
			},
		},
	}
	for _, tc := range testcases {
		t.Run(tc.scenario, tc.exec)
	}
}
