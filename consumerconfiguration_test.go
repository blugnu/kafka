package kafka

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/blugnu/kafka/context"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func TestConsumerConfiguration(t *testing.T) {
	// ARRANGE
	type result struct {
		*Config
		*consumer
		error
	}
	p := &producer{}

	testcases := []struct {
		name string
		sut  ConsumerConfiguration
		result
	}{
		{name: "auto offset reset", sut: AutoOffsetReset(OffsetResetEarliest), result: result{
			Config: &Config{
				ConfigMap: kafka.ConfigMap{"auto.offset.reset": "earliest"}},
			consumer: &consumer{},
		}},
		{name: "group id", sut: GroupId("group"), result: result{
			Config: &Config{
				ConfigMap: kafka.ConfigMap{"group.id": "group"}},
			consumer: &consumer{groupId: "group"},
		}},
		{name: "read timeout", sut: ReadTimeout(1 * time.Hour), result: result{
			Config:   &Config{ConfigMap: kafka.ConfigMap{}},
			consumer: &consumer{readTimeout: 1 * time.Hour},
		}},
		{name: "using producer", sut: WithProducer(p), result: result{
			Config: &Config{
				ConfigMap: kafka.ConfigMap{}},
			consumer: &consumer{Producer: p},
		}},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			// ARRANGE
			cfg := &Config{ConfigMap: kafka.ConfigMap{}}
			con := &consumer{}

			// ACT
			err := tc.sut(cfg, con)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			// ASSERT
			wanted := tc.result
			got := result{Config: cfg, consumer: con}
			if !reflect.DeepEqual(wanted, got) {
				t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
			}
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
		sut    ConsumerConfiguration
		result error
	}{
		{name: "auto offset reset (earliest)", sut: AutoOffsetReset("earliest"), result: cfgerr},
		{name: "auto offset reset (latest)", sut: AutoOffsetReset("latest"), result: cfgerr},
		{name: "auto offset reset (none)", sut: AutoOffsetReset("none"), result: cfgerr},
		{name: "auto offset reset (invalid)", sut: AutoOffsetReset("invalid"), result: cfgerr},
		{name: "group id", sut: GroupId("group"), result: cfgerr},
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

func TestHandlerConfigurationRequiresHandlerFunction(t *testing.T) {
	// ARRANGE
	cfg := &Config{}
	con := &consumer{
		handlers: map[string]*handler{},
	}

	// ACT
	got := Handlers(map[string]Handler{"test": {}})(cfg, con)

	// ASSERT
	wanted := ErrHandlerFunctionIsRequired
	if !errors.Is(got, wanted) {
		t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
	}
}

func TestHandlerConfigurationAppliesDefaults(t *testing.T) {
	// ARRANGE
	cfg := &Config{}
	con := &consumer{
		handlers: map[string]*handler{},
	}

	// ACT
	err := Handlers(map[string]Handler{"test": {Function: func(context.Context, *kafka.Message) error { return nil }}})(cfg, con)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// ASSERT
	t.Run("encryption handler", func(t *testing.T) {
		wanted := Default.EncryptionHandler
		got := con.handlers["test"].EncryptionHandler
		if wanted != got {
			t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
		}
	})

	t.Run("error handler", func(t *testing.T) {
		wanted := Default.ConsumerErrorHandler
		got := con.handlers["test"].OnErrorHandler
		if wanted != got {
			t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
		}
	})
}

func TestHandlerConfigurationAddsHandlerForRetryTopics(t *testing.T) {
	// ARRANGE
	cfg := &Config{}
	con := &consumer{
		handlers: map[string]*handler{},
	}

	// ACT
	err := Handlers(map[string]Handler{
		"test": {
			Function: func(context.Context, *kafka.Message) error { return nil },
			OnError: &Retry[string]{
				Topic: "retry",
			},
		},
	})(cfg, con)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// ASSERT
	wanted := con.handlers["test"]
	got := con.handlers["retry"]
	if !reflect.DeepEqual(wanted, got) {
		t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
	}
}

func TestHandlerConfigurationAppliesRetryDefaults(t *testing.T) {
	// ARRANGE
	cfg := &Config{}
	con := &consumer{
		handlers: map[string]*handler{},
	}

	// ACT
	err := Handlers(map[string]Handler{
		"test": {
			Function: func(context.Context, *kafka.Message) error { return nil },
			OnError: &Retry[string]{
				Topic: "retry",
			},
		},
	})(cfg, con)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// ASSERT
	wanted := &Retry[string]{
		Topic:           "retry",
		MetadataHandler: DefaultRetryMetadataHandler(),
		Throttling:      UnthrottledRetries(),
		consumer:        con,
	}
	got := con.handlers["test"].OnErrorHandler.(*Retry[string])
	if !reflect.DeepEqual(wanted, got) {
		t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
	}
}
