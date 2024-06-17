package kafka

import (
	"context"
	"errors"
	"maps"
	"testing"
	"time"

	"github.com/blugnu/test"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func TestCheckDeliveryEvent(t *testing.T) {
	// ARRANGE
	msgerr := errors.New("message error")
	kaferr := kafka.NewError(kafka.ErrAllBrokersDown, "fake kafka error", true)

	type result struct {
		tp *kafka.TopicPartition
		error
	}

	testcases := []struct {
		name  string
		event kafka.Event
		result
	}{
		{name: "message", event: &kafka.Message{}, result: result{tp: &kafka.TopicPartition{}}},
		{name: "message with error", event: &kafka.Message{TopicPartition: kafka.TopicPartition{Error: msgerr}}, result: result{tp: &kafka.TopicPartition{Error: msgerr}, error: msgerr}},
		{name: "error", event: kaferr, result: result{error: kaferr}},
		{name: "unexpected event", event: csPanic, result: result{error: UnexpectedDeliveryEvent{csPanic}}},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			// ACT
			tp, err := deliveryEvent(tc.event)

			// ASSERT
			test.That(t, result{tp: tp, error: err}).Equals(tc.result)
		})
	}
}

func TestProducerMustProduceWhenEncryptionFails(t *testing.T) {
	// ARRANGE
	ctx := context.Background()
	encerr := errors.New("encryption error")
	msg := kafka.Message{}

	sut := &producer{}
	sut.cypher.encrypt = func(context.Context, *kafka.Message) error { return encerr }
	sut.funcs.prepareMessage = func(context.Context, *kafka.Message) error { return nil }

	// ACT
	result, err := sut.MustProduce(ctx, msg)

	// ASSERT
	t.Run("result", func(t *testing.T) {
		got := result
		if got != nil {
			t.Errorf("\nwanted nil\ngot    %T", got)
		}
	})

	t.Run("error", func(t *testing.T) {
		wanted := encerr
		got := err
		if !errors.Is(got, wanted) {
			t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
		}
	})
}

func TestProducerMustProduce(t *testing.T) {
	// ARRANGE
	ctx := context.Background()
	msg := kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     test.AddressOf("topic"),
			Partition: kafka.PartitionAny,
			Offset:    kafka.OffsetEnd,
		},
	}

	prderr := errors.New("producer error")
	kfkerr := kafka.NewError(kafka.ErrAllBrokersDown, "fake kafka error", true)
	msgerr := errors.New("message error")

	sut := &producer{}
	sut.cypher = noEncryption
	sut.funcs.prepareMessage = func(context.Context, *kafka.Message) error { return nil }
	sut.funcs.checkDelivery = func(context.Context, *kafka.Message, error) {}

	testcases := []struct {
		name   string
		prderr error
		event  kafka.Event
		msgerr error
		result *kafka.TopicPartition
		error
	}{
		{name: "when message is delivered ok", result: &kafka.TopicPartition{Topic: test.AddressOf("topic"), Partition: 1, Offset: 1492}},
		{name: "when producer fails", prderr: prderr, error: prderr},
		{name: "when delivery fails", event: kfkerr, error: kfkerr},
		{name: "when message is delivered with error", event: &kafka.Message{TopicPartition: kafka.TopicPartition{Error: msgerr}}, error: msgerr},
		{name: "when delivery event is unexpected", event: csPanic, error: UnexpectedDeliveryEvent{csPanic}},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			// ARRANGE
			sut.funcs.produce = func(msg *kafka.Message, dc chan kafka.Event) error {
				if tc.prderr != nil {
					return prderr
				}

				ev := tc.event
				if ev == nil && tc.result != nil {
					msg := *msg
					msg.TopicPartition = *tc.result
					ev = &msg
				}
				go func() { dc <- ev }()
				return nil
			}

			// ACT
			result, err := sut.MustProduce(ctx, msg)

			// ASSERT
			test.Error(t, err).Is(tc.error)
			test.That(t, result).Equals(tc.result)
		})
	}
}

func TestProducer_MustProduce_TimeoutRetries(t *testing.T) {
	// ARRANGE
	ctx := context.Background()
	msg := kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     test.AddressOf("topic"),
			Partition: kafka.PartitionAny,
			Offset:    kafka.OffsetEnd,
		},
	}
	log := []struct {
		msg  string
		info LogInfo
	}{}
	defer test.Using(&logs.Debug, func(ctx context.Context, msg string, info LogInfo) {
		log = append(log, struct {
			msg  string
			info LogInfo
		}{msg: msg, info: info})
	})()

	sut := &producer{
		maxRetries: 2,
	}
	sut.cypher = noEncryption
	sut.funcs.prepareMessage = func(context.Context, *kafka.Message) error { return nil }
	sut.funcs.checkDelivery = func(context.Context, *kafka.Message, error) {}

	produceFuncCalls := 0
	sut.funcs.produce = func(msg *kafka.Message, dc chan kafka.Event) error {
		produceFuncCalls++
		go func() { dc <- kafka.NewError(kafka.ErrMsgTimedOut, "fake timeout", true) }()
		return nil
	}

	// ACT
	_, err := sut.MustProduce(ctx, msg)

	// ASSERT
	test.Error(t, err).Is(ErrTimeout)
	test.That(t, produceFuncCalls).Equals(3) // original call + 2 retry attempts
}

func TestProducerMessagePipeline(t *testing.T) {
	// ARRANGE
	ctx := context.Background()

	produceCalled := false

	sut := &producer{cypher: noEncryption}
	sut.funcs.produce = func(msg *kafka.Message, dc chan kafka.Event) error {
		produceCalled = true
		go func() {
			dc <- msg
		}()
		return nil
	}

	// ARRANGE
	testcases := []struct {
		scenario string
		exec     func(t *testing.T)
	}{
		{scenario: "default pipeline",
			exec: func(t *testing.T) {
				// ARRANGE
				_ = sut.configure(&Config{config: kafka.ConfigMap{}})
				msg := kafka.Message{}
				defer test.Using(&deliveryEvent, func(kafka.Event) (*kafka.TopicPartition, error) {
					return &kafka.TopicPartition{
						Topic: test.AddressOf("topic"),
					}, nil
				})()

				// ACT
				_, err := sut.MustProduce(ctx, msg)

				// ASSERT
				// there is not much we can test here as we are primarily exercising the
				// default pipeline functions which do nothing; all we can really test is
				// that we get no error and no panic
				test.That(t, err).IsNil()
			},
		},
		{scenario: "custom pipeline",
			exec: func(t *testing.T) {
				// ARRANGE
				msg := kafka.Message{}
				beforeCalled := false
				afterCalled := false
				encryptCalled := false
				produceCalled = false
				defer test.Using(&sut.cypher.encrypt, func(ctx context.Context, msg *kafka.Message) error { encryptCalled = true; return nil })()
				defer test.Using(&sut.funcs.prepareMessage, func(context.Context, *kafka.Message) error { beforeCalled = true; return nil })()
				defer test.Using(&sut.funcs.checkDelivery, func(context.Context, *kafka.Message, error) { afterCalled = true })()
				defer test.Using(&deliveryEvent, func(kafka.Event) (*kafka.TopicPartition, error) {
					return &kafka.TopicPartition{
						Topic: test.AddressOf("topic"),
					}, nil
				})()

				// ACT
				_, err := sut.MustProduce(ctx, msg)

				// ASSERT
				test.That(t, err).IsNil()
				test.IsTrue(t, beforeCalled, "calls pipeline.BeforeProduction")
				test.IsTrue(t, encryptCalled, "calls encrypt()")
				test.IsTrue(t, produceCalled, "calls produce()")
				test.IsTrue(t, afterCalled, "calls pipeline.AfterDelivery")
			},
		},
		{scenario: "custom pipeline with error",
			exec: func(t *testing.T) {
				// ARRANGE
				preperr := errors.New("prepare error")
				msg := kafka.Message{}
				beforeCalled := false
				afterCalled := false
				encryptCalled := false
				produceCalled = false
				defer test.Using(&sut.cypher.encrypt, func(ctx context.Context, msg *kafka.Message) error { encryptCalled = true; return nil })()
				defer test.Using(&sut.funcs.prepareMessage, func(context.Context, *kafka.Message) error { beforeCalled = true; return preperr })()
				defer test.Using(&sut.funcs.checkDelivery, func(context.Context, *kafka.Message, error) { afterCalled = true })()

				// ACT
				_, err := sut.MustProduce(ctx, msg)

				// ASSERT
				test.That(t, err).IsNotNil()
				test.IsTrue(t, beforeCalled, "calls pipeline.BeforeProduction")
				test.IsFalse(t, encryptCalled, "calls encrypt()")
				test.IsFalse(t, produceCalled, "calls produce()")
				test.IsFalse(t, afterCalled, "calls pipeline.AfterDelivery")
			},
		},
	}
	for _, tc := range testcases {
		t.Run(tc.scenario, func(t *testing.T) {
			tc.exec(t)
		})
	}
}

func TestProducerConfigurationErrors(t *testing.T) {
	// ARRANGE
	cfgerr := errors.New("configuration error")

	testcases := []struct {
		scenario string
		exec     func(t *testing.T)
	}{
		{scenario: "delivery.timeout",
			exec: func(t *testing.T) {
				// ARRANGE
				defer test.Using(&setKeyFn, func(cfg kafka.ConfigMap, k string, v any) error {
					if k == "delivery.timeout.ms" {
						return cfgerr
					}
					return nil
				})()

				// ACT
				result, err := NewProducer(context.Background(), &Config{})

				// ASSERT
				test.Error(t, err).Is(cfgerr)
				test.That(t, result).IsNil()
			},
		},
		{scenario: "enable.idempotence",
			exec: func(t *testing.T) {
				// ARRANGE
				defer test.Using(&setKeyFn, func(cfg kafka.ConfigMap, k string, v any) error {
					if k == "enable.idempotence" {
						return cfgerr
					}
					return nil
				})()

				// ACT
				result, err := NewProducer(context.Background(), &Config{})

				// ASSERT
				test.Error(t, err).Is(cfgerr)
				test.That(t, result).IsNil()
			},
		},
		{scenario: "go.logs.channel.enable",
			exec: func(t *testing.T) {
				// ARRANGE
				defer test.Using(&setKeyFn, func(cfg kafka.ConfigMap, k string, v any) error {
					if k == "go.logs.channel.enable" {
						return cfgerr
					}
					return nil
				})()

				// ACT
				result, err := NewProducer(context.Background(), &Config{})

				// ASSERT
				test.Error(t, err).Is(cfgerr)
				test.That(t, result).IsNil()
			},
		},
	}
	for _, tc := range testcases {
		t.Run(tc.scenario, func(t *testing.T) {
			tc.exec(t)
		})
	}
}

func TestNewProducerClonesTheBaseConfig(t *testing.T) {
	// ARRANGE
	ctx := context.Background()
	cfg := &Config{config: kafka.ConfigMap{}}

	og := createProducer
	defer func() { createProducer = og }()

	createProducer = func(cfg *kafka.ConfigMap) (*kafka.Producer, error) {
		return &kafka.Producer{}, nil
	}

	// ACT
	_, err := NewProducer(ctx, cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// ASSERT
	wanted := kafka.ConfigMap{}
	got := cfg.config
	if !maps.Equal(wanted, got) {
		t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
	}
}

func TestNewProducerAddsDefaultConfiguration(t *testing.T) {
	// ARRANGE
	ctx := context.Background()
	cfg := &Config{config: kafka.ConfigMap{}}

	og := createProducer
	defer func() { createProducer = og }()

	var cfgapplied kafka.ConfigMap
	createProducer = func(cfg *kafka.ConfigMap) (*kafka.Producer, error) {
		cfgapplied = *cfg
		return &kafka.Producer{}, nil
	}

	// ACT
	_, err := NewProducer(ctx, cfg)

	// ASSERT
	test.That(t, err).IsNil()
	test.That(t, cfgapplied).Equals(kafka.ConfigMap{
		"delivery.timeout.ms":    5000,
		"enable.idempotence":     true,
		"go.logs.channel.enable": true,
	})
}

func TestNewProducerEnforcesRequiredConfiguration(t *testing.T) {
	// ARRANGE
	ctx := context.Background()
	cfg := &Config{config: kafka.ConfigMap{
		"enable.idempotence":     false,
		"go.logs.channel.enable": false,
	}}

	og := createProducer
	defer func() { createProducer = og }()

	var cfgapplied kafka.ConfigMap
	createProducer = func(cfg *kafka.ConfigMap) (*kafka.Producer, error) {
		cfgapplied = *cfg
		return &kafka.Producer{}, nil
	}

	// ACT
	_, err := NewProducer(ctx, cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// ASSERT
	test.That(t, err).IsNil()
	test.That(t, cfgapplied).Equals(kafka.ConfigMap{
		"delivery.timeout.ms":    5000,
		"enable.idempotence":     true,
		"go.logs.channel.enable": true,
	})
}

func TestNewProducerReturnsProducerConfigurationErrors(t *testing.T) {
	// ARRANGE
	ctx := context.Background()
	cfg := &Config{config: kafka.ConfigMap{}}
	cfgerr := errors.New("configuration error")

	opt := func(_ *Config, _ *producer) error {
		return cfgerr
	}

	// ACT
	_, got := NewProducer(ctx, cfg, opt)

	// ASSERT
	wanted := ConfigurationError{}
	if !errors.Is(got, wanted) {
		t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
	}
}

func TestNewProducerErrors(t *testing.T) {
	// ARRANGE
	ctx := context.Background()
	cfg := &Config{config: kafka.ConfigMap{}}
	createerr := errors.New("create error")

	og := createProducer
	defer func() { createProducer = og }()
	createProducer = func(cfg *kafka.ConfigMap) (*kafka.Producer, error) { return nil, createerr }

	// ACT
	_, got := NewProducer(ctx, cfg)

	// ASSERT
	wanted := createerr
	if !errors.Is(got, wanted) {
		t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
	}
}

func TestProducerLogsChannelGoRoutineTerminatesWhenChannelIsClosed(t *testing.T) {
	// ARRANGE
	ctx := context.Background()
	cfg := &Config{config: kafka.ConfigMap{}}

	og := createProducer
	defer func() { createProducer = og }()
	createProducer = func(cfg *kafka.ConfigMap) (*kafka.Producer, error) { return &kafka.Producer{}, nil }

	// ACT
	con, err := NewProducer(ctx, cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	producer := con.(*producer)
	producer.logsChannel = make(chan kafka.LogEvent)
	func() {
		ctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel()

		close(producer.logsChannel)

		for {
			select {
			case <-ctx.Done():
				return
			default:
				if producer.logsChannel == nil {
					return
				}
				time.Sleep(10 * time.Millisecond)
			}
		}
	}()

	// ASSERT
	wanted := true
	got := producer.logsChannel == nil
	if wanted != got {
		t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
	}
}

func TestNewMockProducer(t *testing.T) {
	_, _ = NewMockProducer[int](context.Background())
}

func TestProducer_Err_NilProducer(t *testing.T) {
	// ARRANGE
	var sut *producer

	// ACT
	err := sut.Err()

	// ASSERT
	test.Error(t, err).Is(ErrInvalidOperation)
}

func TestProducer_Err(t *testing.T) {
	// ARRANGE
	sut := &producer{err: errors.New("error")}

	// ACT
	err := sut.Err()

	// ASSERT
	test.Error(t, err).Is(sut.err)
}
