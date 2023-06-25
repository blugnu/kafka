package mock

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func TestMockProducerExpectedToProduceAnyMessageToTopic(t *testing.T) {
	// ARRANGE
	ctx := context.Background()

	mock := &Producer[string]{}
	mock.Expect("topic")

	topic := "topic"
	msg := &kafka.Message{
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
		if !reflect.DeepEqual(wanted, got) {
			t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
		}
	})
	t.Run("expectations", func(t *testing.T) {
		wanted := true
		got, err := mock.ExpectationsMet()
		if wanted != got {
			t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
			t.Errorf("\n%s", err)
		}
	})
}

func TestMockProducerExpectedToProduceMultipleMessagesToTopic(t *testing.T) {
	// ARRANGE
	ctx := context.Background()

	mock := &Producer[string]{}
	mock.Expect("topic")
	mock.Expect("topic")
	mock.Expect("topic")

	topic := "topic"
	msg := &kafka.Message{
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
				if !reflect.DeepEqual(wanted, got) {
					t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
				}
			})
		})
	}

	t.Run("expectations", func(t *testing.T) {
		wanted := true
		got, err := mock.ExpectationsMet()
		if wanted != got {
			t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
			t.Errorf("\n%s", err)
		}
	})
}

func TestMockProducerExpectedToReturnError(t *testing.T) {
	// ARRANGE
	ctx := context.Background()

	perr := errors.New("producer error")
	mock := &Producer[string]{}
	mock.Expect("topic").ReturnsError(perr)

	msg := &kafka.Message{}

	// ACT
	off, err := mock.MustProduce(ctx, msg)

	// ASSERT
	t.Run("result", func(t *testing.T) {
		got := off
		if got != nil {
			t.Errorf("\nwanted nil\ngot    %T", got)
		}
	})

	t.Run("error", func(t *testing.T) {
		wanted := perr
		got := err
		if !errors.Is(got, wanted) {
			t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
		}
	})

	t.Run("expectations", func(t *testing.T) {
		wanted := true
		got, err := mock.ExpectationsMet()
		if wanted != got {
			t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
			t.Errorf("\n%s", err)
		}
	})
}

func TestMockProducerFailsToProduceExpectedMessage(t *testing.T) {
	// ARRANGE
	ctx := context.Background()

	mock := &Producer[string]{}
	mock.Expect("topic")

	topic := "other.topic"
	msg := &kafka.Message{
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
	t.Run("produces message", func(t *testing.T) {
		wanted := &kafka.TopicPartition{Topic: &topic, Partition: 0, Offset: 0}
		got := off
		if !reflect.DeepEqual(wanted, got) {
			t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
		}
	})

	t.Run("expectations not met", func(t *testing.T) {
		wanted := false
		got, err := mock.ExpectationsMet()
		if wanted != got {
			t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
		}

		t.Run("with error", func(t *testing.T) {
			wanted := "\nexpectation 1 not met:\n  expected : topic=\"topic\", key=<any>, headers=<any>, value=<any>\n  got      : topic=\"other.topic\", key=<none>, headers=<none>, value=<none>"
			got := err.Error()
			if wanted != got {
				t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
			}
		})
	})
}

func TestMockProducerProducesUnexpectedMessage(t *testing.T) {
	// ARRANGE
	ctx := context.Background()

	mock := &Producer[string]{}

	topic := "topic"
	msg := &kafka.Message{
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
	t.Run("produces message", func(t *testing.T) {
		wanted := &kafka.TopicPartition{Topic: &topic, Partition: 0, Offset: 0}
		got := off
		if !reflect.DeepEqual(wanted, got) {
			t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
		}
	})

	t.Run("expectations not met", func(t *testing.T) {
		wanted := false
		got, err := mock.ExpectationsMet()
		if wanted != got {
			t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
		}

		t.Run("with error", func(t *testing.T) {
			wanted := "produced unexpected message: topic=\"topic\", key=<none>, headers=<none>, value=<none>"
			got := err.Error()
			if wanted != got {
				t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
			}
		})
	})
}

func TestMockProducerReset(t *testing.T) {
	// ARRANGE
	sut := &Producer[string]{
		expectations: []*expectation{{}},
		unexpected:   []*kafka.Message{{}},
		next:         1,
		error:        errors.New("error"),
	}

	// ACT
	sut.Reset()

	// ASSERT
	wanted := &Producer[string]{}
	got := sut
	if !reflect.DeepEqual(wanted, got) {
		t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
	}
}
