package kafka

import (
	"context"
	"errors"
	"maps"
	"testing"

	"github.com/blugnu/test"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func (c cypher) Decrypt(ctx context.Context, msg *kafka.Message) error {
	return c.decrypt(ctx, msg)
}

func (c cypher) Encrypt(ctx context.Context, msg *kafka.Message) error {
	return c.encrypt(ctx, msg)
}

func TestConfig_clone(t *testing.T) {
	// ARRANGE
	sut := &Config{config: kafka.ConfigMap{
		"key.1": "value-1",
		"key.2": "value-2",
	}}

	// ACT
	got := sut.clone()

	// ASSERT
	wanted := sut
	if !maps.Equal(wanted.config, got.config) {
		t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
	}
}

func TestConfig(t *testing.T) {
	// ARRANGE
	testcases := []struct {
		scenario string
		exec     func(t *testing.T)
	}{
		{scenario: "Cypher",
			exec: func(t *testing.T) {
				// ARRANGE
				cfg := &Config{}

				// ACT
				err := Cypher(cypher{
					decrypt: func(ctx context.Context, msg *kafka.Message) error { return nil },
					encrypt: func(ctx context.Context, msg *kafka.Message) error { return nil },
				})(cfg)

				// ASSERT
				test.That(t, err).IsNil()
				test.That(t, cfg.cypher.decrypt).IsNotNil()
				test.That(t, cfg.cypher.encrypt).IsNotNil()
			},
		},
		{scenario: "config map changes",
			exec: func(t *testing.T) {
				// ARRANGE
				testcases := []struct {
					name   string
					sut    ConfigOption
					result kafka.ConfigMap
				}{
					{name: "BootstrapServers", sut: BootstrapServers("localhost:8080"), result: kafka.ConfigMap{"bootstrap.servers": "localhost:8080"}},
					{name: "Broker", sut: Broker("localhost", 8080), result: kafka.ConfigMap{"bootstrap.servers": "localhost:8080"}},
					{name: "SASL", sut: SASL("mechanisms", "username", "password"), result: kafka.ConfigMap{"sasl.mechanisms": "mechanisms", "sasl.username": "username", "sasl.password": "password"}},
					{name: "SecurityProtocol", sut: SecurityProtocol("protocol"), result: kafka.ConfigMap{"security.protocol": "protocol"}},
					{name: "SSLCALocation", sut: SSLCALocation("location"), result: kafka.ConfigMap{"ssl.ca.location": "location"}},
					{name: "COnfigKey", sut: ConfigKey("key", "value"), result: kafka.ConfigMap{"key": "value"}},
				}
				for _, tc := range testcases {
					t.Run(tc.name, func(t *testing.T) {
						// ARRANGE
						cfg := &Config{config: kafka.ConfigMap{}}

						// ACT
						err := tc.sut(cfg)

						// ASSERT
						test.That(t, err).IsNil()
						test.Map(t, cfg.config).Equals(tc.result)
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

func TestConfig_ErrorHandling(t *testing.T) {
	// ARRANGE
	cfgerr := errors.New("configuration error")
	og := setKeyFn
	defer func() { setKeyFn = og }()
	setKeyFn = func(cfg kafka.ConfigMap, key string, value any) error { return cfgerr }

	testcases := []struct {
		name   string
		sut    ConfigOption
		result error
	}{
		{name: "bootstrap servers", sut: BootstrapServers("localhost:8080"), result: cfgerr},
		{name: "broker", sut: Broker("localhost", 8080), result: cfgerr},
		{name: "config key", sut: ConfigKey("key", "value"), result: cfgerr},
		{name: "security protocol", sut: SecurityProtocol("protocol"), result: cfgerr},
		{name: "ssl ca location", sut: SSLCALocation("location"), result: cfgerr},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			// ARRANGE
			cfg := &Config{}

			// ACT
			got := tc.sut(cfg)

			// ASSERT
			wanted := tc.result
			if !errors.Is(got, wanted) {
				t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
			}
		})
	}
}

func TestConfig_SaslErrorHandling(t *testing.T) {
	// ARRANGE
	cfgerr := errors.New("configuration error")

	og := setKeyFn
	defer func() { setKeyFn = og }()

	testcases := []struct {
		key    string
		result error
	}{
		{key: "sasl.mechanisms", result: cfgerr},
		{key: "sasl.username", result: cfgerr},
		{key: "sasl.password", result: cfgerr},
	}
	for _, tc := range testcases {
		t.Run(tc.key, func(t *testing.T) {
			// ARRANGE
			setKeyFn = func(cfg kafka.ConfigMap, key string, value any) error {
				if key == tc.key {
					return cfgerr
				}
				return nil
			}
			cfg := &Config{}

			// ACT
			got := SASL("mechanisms", "username", "password")(cfg)

			// ASSERT
			wanted := tc.result
			if !errors.Is(got, wanted) {
				t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
			}
		})
	}
}

func TestNewConfig_ReturnsConfigurationErrors(t *testing.T) {
	// ARRANGE
	cfgerr := errors.New("configuration error")
	cfg := func() ConfigOption { return func(cfg *Config) error { return cfgerr } }

	// ACT
	got, err := NewConfig(cfg())

	// ASSERT
	t.Run("result", func(t *testing.T) {
		if got != nil {
			t.Errorf("wanted nil, got %#v", got)
		}
	})

	t.Run("error", func(t *testing.T) {
		wanted := cfgerr
		got := err
		if !errors.Is(got, wanted) {
			t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
		}
	})
}

func TestNewConfig_AppliesConfiguration(t *testing.T) {
	// ARRANGE
	cfg := func() ConfigOption {
		return func(cfg *Config) error {
			if err := cfg.config.SetKey("key", "value"); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			return nil
		}
	}

	// ACT
	got, err := NewConfig(cfg())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// ASSERT
	wanted := &Config{config: kafka.ConfigMap{"key": "value"}}
	if !maps.Equal(wanted.config, got.config) {
		t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
	}
}
