package kafka_test

import (
	"context"
	"errors"
	"testing"

	"github.com/blugnu/kafka"
	"github.com/blugnu/test"
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
				sut := kafka.HandlerFunc(nil)

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
				sut := kafka.HandlerFunc(func(ctx context.Context, m *kafka.Message) error {
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

func TestIf(t *testing.T) {
	// ARRANGE
	ctx := context.Background()

	testcases := []struct {
		scenario string
		exec     func(t *testing.T)
	}{
		{scenario: "condition met",
			exec: func(t *testing.T) {
				// ARRANGE
				handlerIsCalled := false
				condition := func(m *kafka.Message) bool {
					return true
				}
				handler := kafka.HandlerFunc(func(ctx context.Context, m *kafka.Message) error {
					handlerIsCalled = true
					return nil
				})

				// ACT
				err := kafka.If(condition, handler).HandleMessage(ctx, nil)

				// ASSERT
				test.That(t, err).IsNil()
				test.IsTrue(t, handlerIsCalled)
			},
		},
		{scenario: "condition not met",
			exec: func(t *testing.T) {
				// ARRANGE
				handlerIsCalled := false
				condition := func(m *kafka.Message) bool {
					return false
				}
				handler := kafka.HandlerFunc(func(ctx context.Context, m *kafka.Message) error {
					handlerIsCalled = true
					return nil
				})

				// ACT
				err := kafka.If(condition, handler).HandleMessage(ctx, nil)

				// ASSERT
				test.That(t, err).IsNil()
				test.IsFalse(t, handlerIsCalled)
			},
		},
	}
	for _, tc := range testcases {
		t.Run(tc.scenario, func(t *testing.T) {
			tc.exec(t)
		})
	}
}
