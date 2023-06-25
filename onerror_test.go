package kafka

import (
	"context"
	"errors"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func TestOnError(t *testing.T) {
	// ARRANGE
	ctx := context.Background()
	msg := &kafka.Message{}
	err := errors.New("on error, error")

	sut := &onerror{err}

	// ACT
	got := sut.HandleError(ctx, msg, errors.New("handler error"))

	// ASSERT
	wanted := err
	if wanted != got {
		t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
	}
}

func TestPredefinedErrorHandlers(t *testing.T) {
	// ARRANGE
	ctx := context.Background()
	msg := &kafka.Message{}

	testcases := []struct {
		name   string
		sut    OnErrorHandler
		result error
	}{
		{name: "halt consumer", sut: HaltConsumer(), result: FatalError{}},
		{name: "reprocess message", sut: ReprocessMessage(), result: ErrReprocessMessage},
		{name: "skip message", sut: SkipMessage(), result: nil},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			// ACT
			got := tc.sut.HandleError(ctx, msg, errors.New("handler error"))

			// ASSERT
			wanted := tc.result
			if !errors.Is(got, wanted) {
				t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
			}
		})
	}
}
