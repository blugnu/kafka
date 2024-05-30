package kafka

import (
	"context"
	"errors"
	"testing"

	"github.com/blugnu/test"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func TestHandlerFunc(t *testing.T) {
	// ARRANGE
	ctx := context.Background()

	testcases := []struct {
		scenario string
		exec     func(t *testing.T)
	}{
		{scenario: "nil",
			exec: func(t *testing.T) {
				// ARRANGE
				sut := HandlerFunc(nil)

				// ACT
				err := sut.HandleMessage(ctx, nil)

				// ASSERT
				test.That(t, err).IsNil()
			},
		},
		{scenario: "not nil",
			exec: func(t *testing.T) {
				// ARRANGE
				sentinel := errors.New("error")
				sut := HandlerFunc(func(ctx context.Context, m *kafka.Message) error {
					return sentinel
				})

				// ACT
				err := sut.HandleMessage(ctx, nil)

				// ASSERT
				test.Error(t, err).Is(sentinel)
			},
		},
	}
	for _, tc := range testcases {
		t.Run(tc.scenario, func(t *testing.T) {
			tc.exec(t)
		})
	}
}
