package kafka //nolint: testpackage // testing private functions and types

import (
	"context"
	"testing"
	"time"

	"github.com/blugnu/test"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func TestProducerOptions(t *testing.T) {
	// ARRANGE
	testcases := []struct {
		scenario string
		exec     func(t *testing.T)
	}{
		{scenario: "DeliveryTimeout",
			exec: func(t *testing.T) {
				// ARRANGE
				cfg := &Config{config: kafka.ConfigMap{}}

				// ACT
				err := DeliveryTimeout(10*time.Millisecond)(cfg, nil)

				// ASSERT
				test.That(t, err).IsNil()

				if timeout, ok := test.IsType[int](t, cfg.config["delivery.timeout.ms"]); ok {
					test.That(t, timeout).Equals(10)
				}
			},
		},
		{scenario: "MaxRetries",
			exec: func(t *testing.T) {
				// ARRANGE
				cfg := &Config{}
				p := &producer{}

				// ACT
				err := MaxRetries(10)(cfg, p)

				// ASSERT
				test.That(t, err).IsNil()
				test.That(t, p.maxRetries).Equals(10)
			},
		},
		{scenario: "MessageEncryption",
			exec: func(t *testing.T) {
				// ARRANGE
				cfg := &Config{}
				fn := func(context.Context, *Message) error { return nil }

				// ACT
				err := MessageEncryption(fn)(cfg, nil)

				// ASSERT
				test.That(t, err).IsNil()
				test.That(t, cfg.cypher.encrypt).IsNotNil()
			},
		},
		{scenario: "MessagePipeline",
			exec: func(t *testing.T) {
				// ARRANGE
				p := &producer{funcs: struct {
					checkDelivery  func(context.Context, *Message, error)
					prepareMessage func(context.Context, *Message) error
					produce        func(*kafka.Message, chan kafka.Event) error
				}{}}
				pipeline := ProducerPipeline{
					BeforeProduction: HandlerFunc(func(context.Context, *Message) error { return nil }),
					AfterDelivery:    func(context.Context, *Message, error) {},
				}

				// ACT
				err := MessagePipeline(pipeline)(nil, p)

				// ASSERT
				test.That(t, err).IsNil()
				test.That(t, p.funcs.prepareMessage).IsNotNil()
				test.That(t, p.funcs.checkDelivery).IsNotNil()
			},
		},
	}
	for _, tc := range testcases {
		t.Run(tc.scenario, func(t *testing.T) {
			tc.exec(t)
		})
	}
}
