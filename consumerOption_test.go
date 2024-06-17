package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/blugnu/test"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func TestConsumerOptions(t *testing.T) {
	// ARRANGE
	// ARRANGE
	testcases := []struct {
		scenario string
		exec     func(t *testing.T)
	}{
		{scenario: "MessageDecryption",
			exec: func(t *testing.T) {
				// ARRANGE
				cfg := &Config{}
				con := &consumer{}
				fn := func(ctx context.Context, m *kafka.Message) error { return nil }

				// ACT
				err := MessageDecryption(fn)(cfg, con)

				// ASSERT
				test.That(t, err).IsNil()
				test.That(t, cfg.decrypt).IsNil()
				test.That(t, con.decrypt).IsNotNil()
			},
		},
		{scenario: "config map",
			exec: func(t *testing.T) {
				type result struct {
					*Config
					*consumer
				}

				testcases := []struct {
					name string
					sut  ConsumerOption
					result
				}{
					{name: "AutoOffsetReset", sut: AutoOffsetReset(OffsetResetEarliest), result: result{
						Config: &Config{
							config: kafka.ConfigMap{"auto.offset.reset": "earliest"}},
						consumer: &consumer{},
					}},
					{name: "ConsumerGroupID", sut: ConsumerGroupID("group"), result: result{
						Config: &Config{
							config: kafka.ConfigMap{"group.id": "group"}},
						consumer: &consumer{groupId: "group"},
					}},
					{name: "ReadTimeout", sut: ReadTimeout(1 * time.Hour), result: result{
						Config:   &Config{config: kafka.ConfigMap{}},
						consumer: &consumer{readTimeout: 1 * time.Hour},
					}},
				}
				for _, tc := range testcases {
					t.Run(tc.name, func(t *testing.T) {
						// ARRANGE
						cfg := &Config{config: kafka.ConfigMap{}}
						con := &consumer{}

						// ACT
						err := tc.sut(cfg, con)

						// ASSERT
						test.That(t, err).IsNil()
						test.That(t, result{Config: cfg, consumer: con}).Equals(tc.result)
					})
				}
			},
		},
	}
	for _, tc := range testcases {
		t.Run(tc.scenario, func(t *testing.T) {
			tc.exec(t)
		})
	}
}

func TestConsumerConfigurationErrorHandling(t *testing.T) {
	// ARRANGE
	cfgerr := errors.New("configuration error")

	og := setKeyFn
	defer func() { setKeyFn = og }()
	setKeyFn = func(kafka.ConfigMap, string, interface{}) error { return cfgerr }

	testcases := []struct {
		name   string
		sut    ConsumerOption
		result error
	}{
		{name: "auto offset reset (earliest)", sut: AutoOffsetReset("earliest"), result: cfgerr},
		{name: "auto offset reset (latest)", sut: AutoOffsetReset("latest"), result: cfgerr},
		{name: "auto offset reset (none)", sut: AutoOffsetReset("none"), result: cfgerr},
		{name: "auto offset reset (invalid)", sut: AutoOffsetReset("invalid"), result: cfgerr},
		{name: "group id", sut: ConsumerGroupID("group"), result: cfgerr},
		{name: "read timeout", sut: ReadTimeout(-2), result: ErrInvalidReadTimeout},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			// ARRANGE
			cfg := &Config{}
			con := &consumer{}

			// ACT
			got := tc.sut(cfg, con)

			// ASSERT
			wanted := tc.result
			if !errors.Is(got, wanted) {
				t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
			}
		})
	}
}

func TestHandlerConfigurationAppliesDefaults(t *testing.T) {
	// ARRANGE
	cfg := &Config{}
	con := &consumer{
		handlers: map[string]Handler{},
	}
	// ACT
	err := TopicHandlers(map[string]Handler{
		"test": HandlerFunc(func(ctx context.Context, m *kafka.Message) error { return nil }),
	})(cfg, con)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// ASSERT
	test.That(t, con.handlers["test"]).IsNotNil()
}
