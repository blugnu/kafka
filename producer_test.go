package kafka

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"golang.org/x/exp/maps"
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
			tp, err := checkDeliveryEvent(tc.event)

			// ASSERT
			wanted := tc.result
			got := result{tp: tp, error: err}
			if !reflect.DeepEqual(wanted, got) {
				t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
			}
		})
	}
}

func TestProducerMustProduceWhenEncryptionFails(t *testing.T) {
	// ARRANGE
	ctx := context.Background()
	encerr := errors.New("encryption error")
	msg := &kafka.Message{}

	sut := &producer{
		EncryptionHandler: &encryptionhandler{
			encrypt: func(*kafka.Message) error {
				return encerr
			},
		},
	}

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
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     addr("topic"),
			Partition: kafka.PartitionAny,
			Offset:    kafka.OffsetEnd,
		},
	}

	prderr := errors.New("producer error")
	kfkerr := kafka.NewError(kafka.ErrAllBrokersDown, "fake kafka error", true)
	msgerr := errors.New("message error")

	sut := &producer{
		EncryptionHandler: &encryptionhandler{
			encrypt: func(*kafka.Message) error { return nil },
		},
	}

	testcases := []struct {
		name   string
		prderr error
		event  kafka.Event
		msgerr error
		result *kafka.TopicPartition
		error
	}{
		{name: "when message is delivered ok", result: &kafka.TopicPartition{Topic: addr("topic"), Partition: 1, Offset: 1492}},
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
			t.Run("result", func(t *testing.T) {
				wanted := tc.result
				got := result
				if !reflect.DeepEqual(wanted, got) {
					t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
				}
			})

			t.Run("error", func(t *testing.T) {
				wanted := tc.error
				got := err
				if !errors.Is(got, wanted) {
					t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
				}
			})
		})
	}
}

func TestNewProducerClonesTheBaseConfig(t *testing.T) {
	// ARRANGE
	ctx := context.Background()
	cfg := &Config{ConfigMap: kafka.ConfigMap{}}

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
	got := cfg.ConfigMap
	if !maps.Equal(wanted, got) {
		t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
	}
}

func TestNewProducerAddsDefaultConfiguration(t *testing.T) {
	// ARRANGE
	ctx := context.Background()
	cfg := &Config{ConfigMap: kafka.ConfigMap{}}

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
	wanted := kafka.ConfigMap{
		"enable.idempotence":     true,
		"go.logs.channel.enable": true,
	}
	got := cfgapplied
	if !maps.Equal(wanted, got) {
		t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
	}
}

func TestNewProducerEnforcesRequiredConfiguration(t *testing.T) {
	// ARRANGE
	ctx := context.Background()
	cfg := &Config{ConfigMap: kafka.ConfigMap{
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
	wanted := kafka.ConfigMap{
		"enable.idempotence":     true,
		"go.logs.channel.enable": true,
	}
	got := cfgapplied
	if !maps.Equal(wanted, got) {
		t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
	}
}

func TestNewProducerReturnsProducerConfigurationErrors(t *testing.T) {
	// ARRANGE
	ctx := context.Background()
	cfg := &Config{ConfigMap: kafka.ConfigMap{}}
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

func TestNewProducerReturnsConfigMapErrors(t *testing.T) {
	// ARRANGE
	ctx := context.Background()
	cfg := &Config{ConfigMap: kafka.ConfigMap{}}
	cfgerr := errors.New("configuration error")

	og := setKeyFn
	defer func() { setKeyFn = og }()

	// ARRANGE
	testcases := []struct {
		entry string
	}{
		{entry: "enable.idempotence"},
		{entry: "go.logs.channel.enable"},
	}
	for _, tc := range testcases {
		t.Run(tc.entry, func(t *testing.T) {
			// ARRANGE
			setKeyFn = func(_ kafka.ConfigMap, key string, value any) error {
				if key == tc.entry {
					return cfgerr
				}
				return nil
			}

			// ACT
			_, got := NewProducer(ctx, cfg)

			// ASSERT
			t.Run("error", func(t *testing.T) {
				wanted := cfgerr
				if !errors.Is(got, wanted) {
					t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
				}
				t.Run("is ConfigurationError", func(t *testing.T) {
					wanted := ConfigurationError{}
					if !errors.Is(got, wanted) {
						t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
					}
				})
			})
		})
	}
}

func TestNewProducerErrors(t *testing.T) {
	// ARRANGE
	ctx := context.Background()
	cfg := &Config{ConfigMap: kafka.ConfigMap{}}
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
	cfg := &Config{ConfigMap: kafka.ConfigMap{}}

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
