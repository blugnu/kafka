package kafka

import (
	"reflect"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func TestNoEncryption(t *testing.T) {
	// ARRANGE
	topic := "topic"
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: 1,
			Offset:    kafka.Offset(1492),
		},
		Key:       []byte("key"),
		Value:     []byte("value"),
		Timestamp: time.Now(),
		Headers: []kafka.Header{
			{Key: "header1", Value: []byte("value1")},
		},
	}

	sut := NoEncryption()

	t.Run("decrypt", func(t *testing.T) {
		// ARRANGE
		og := *msg

		// ACT
		err := sut.Decrypt(msg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// ASSERT
		wanted := og
		got := *msg
		if !reflect.DeepEqual(wanted, got) {
			t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
		}
	})

	t.Run("encrypt", func(t *testing.T) {
		// ARRANGE
		og := *msg

		// ACT
		err := sut.Encrypt(msg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// ASSERT
		wanted := og
		got := *msg
		if !reflect.DeepEqual(wanted, got) {
			t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
		}
	})
}
