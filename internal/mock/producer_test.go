package mock

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/blugnu/test"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func tpaEqual(a, b *kafka.TopicPartition) bool {
	return a.Topic == b.Topic &&
		a.Partition == b.Partition &&
		a.Offset == b.Offset
}

func TestMockProducerExpectedToProduceAnyMessageToTopic(t *testing.T) {
	// ARRANGE
	ctx := context.Background()

	mock := &Producer[string]{}
	mock.Expect("topic")

	topic := "topic"
	msg := kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic: &topic,
		},
	}

	// ACT
	off, err := mock.MustProduce(ctx, msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// ASSERT
	t.Run("result", func(t *testing.T) {
		wanted := &kafka.TopicPartition{Topic: &topic, Partition: 0, Offset: 0}
		got := off
		if !tpaEqual(wanted, got) {
			t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
		}
	})
	test.ExpectationsWereMet(t, mock)
}

func TestMockProducerExpectedToProduceMultipleMessagesToTopic(t *testing.T) {
	// ARRANGE
	ctx := context.Background()

	mock := &Producer[string]{}
	mock.Expect("topic")
	mock.Expect("topic")
	mock.Expect("topic")

	topic := "topic"
	msg := kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic: &topic,
		},
	}

	// ACT
	for i := 1; i <= 3; i++ {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			off, err := mock.MustProduce(ctx, msg)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			// ASSERT
			t.Run("result", func(t *testing.T) {
				wanted := &kafka.TopicPartition{Topic: &topic, Partition: 0, Offset: kafka.Offset(i - 1)}
				got := off
				if !tpaEqual(wanted, got) {
					t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
				}
			})
		})
	}

	test.ExpectationsWereMet(t, mock)
}

func TestMockProducerExpectedToReturnError(t *testing.T) {
	// ARRANGE
	ctx := context.Background()

	perr := errors.New("producer error")
	mock := &Producer[string]{}
	mock.Expect("topic").ReturnsError(perr)

	msg := kafka.Message{}

	// ACT
	off, err := mock.MustProduce(ctx, msg)

	// ASSERT
	test.That(t, off).IsNil()
	test.Error(t, err).Is(perr)
	test.Error(t, mock.Err()).Is(perr)
	test.ExpectationsWereMet(t, mock)
}

func TestMessageProducedToUnexpectedTopic(t *testing.T) {
	// ARRANGE
	ctx := context.Background()

	mock := &Producer[string]{}
	mock.Expect("topic")

	msg := kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic: test.AddressOf("other.topic"),
		},
	}

	// ACT
	result, err := mock.MustProduce(ctx, msg)

	// ASSERT
	test.That(t, err).IsNotNil()
	test.That(t, result).IsNil()

	test.Strings(t, strings.Split(err.Error(), "\n")).Equals([]string{
		"message produced to unexpected topic",
		"  wanted: topic",
		"  got   : other.topic",
	})
}

func TestExpectedMessageNotProduced(t *testing.T) {
	// ARRANGE
	mock := &Producer[string]{}
	mock.Expect("topic")

	// ASSERT
	test := test.Helper(t, func(st *testing.T) {
		test.ExpectationsWereMet(st, mock)
	})

	test.Report.Contains([]string{
		"expectation 1 not met:",
		"  expected : topic=\"topic\", key=<any>, headers=<any>, value=<any>",
		"  got      : <no message>",
	})
}

func TestMockProducerProducesUnexpectedMessage(t *testing.T) {
	// ARRANGE
	ctx := context.Background()

	mock := &Producer[string]{}

	topic := "topic"
	msg := kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic: &topic,
		},
	}

	// ACT
	result, err := mock.MustProduce(ctx, msg)

	// ASSERT
	test.That(t, err).IsNil()
	test.That(t, result).Equals(&kafka.TopicPartition{Topic: &topic, Partition: 0, Offset: 0})

	test := test.Helper(t, func(st *testing.T) {
		test.ExpectationsWereMet(st, mock)
	})
	test.Report.Contains([]string{
		"produced unexpected message: topic=\"topic\", key=<none>, headers=<none>, value=<none>",
	})
}

func TestMockProducerReset(t *testing.T) {
	// ARRANGE
	sut := &Producer[string]{
		expectations: []*Expectation{{}},
		unexpected:   []*kafka.Message{{}},
		next:         1,
		err:          errors.New("error"),
	}

	// ACT
	sut.Reset()

	// ASSERT
	test.That(t, sut).Equals(&Producer[string]{})
}
