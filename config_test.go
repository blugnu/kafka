package kafka

import (
	"errors"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"golang.org/x/exp/maps"
)

func TestConfig_clone(t *testing.T) {
	// ARRANGE
	sut := &Config{ConfigMap: kafka.ConfigMap{
		"key.1": "value-1",
		"key.2": "value-2",
	}}

	// ACT
	got := sut.clone()

	// ASSERT
	wanted := sut
	if !maps.Equal(wanted.ConfigMap, got.ConfigMap) {
		t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
	}
}

func TestConfig(t *testing.T) {
	// ARRANGE
	testcases := []struct {
		name   string
		sut    Configuration
		result kafka.ConfigMap
	}{
		{name: "bootstrap servers", sut: BootstrapServers("localhost:8080"), result: kafka.ConfigMap{"bootstrap.servers": "localhost:8080"}},
		{name: "broker", sut: Broker("localhost", 8080), result: kafka.ConfigMap{"bootstrap.servers": "localhost:8080"}},
		{name: "config key", sut: ConfigKey("key", "value"), result: kafka.ConfigMap{"key": "value"}},
		{name: "sasl", sut: Sasl("mechanisms", "username", "password"), result: kafka.ConfigMap{"sasl.mechanisms": "mechanisms", "sasl.username": "username", "sasl.password": "password"}},
		{name: "security protocol", sut: SecurityProtocol("protocol"), result: kafka.ConfigMap{"security.protocol": "protocol"}},
		{name: "ssl ca location", sut: SslCaLocation("location"), result: kafka.ConfigMap{"ssl.ca.location": "location"}},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			// ARRANGE
			cfg := &Config{ConfigMap: kafka.ConfigMap{}}

			// ACT
			err := tc.sut(cfg)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			// ASSERT
			wanted := tc.result
			got := cfg.ConfigMap
			if !maps.Equal(wanted, got) {
				t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
			}
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
		sut    Configuration
		result error
	}{
		{name: "bootstrap servers", sut: BootstrapServers("localhost:8080"), result: cfgerr},
		{name: "broker", sut: Broker("localhost", 8080), result: cfgerr},
		{name: "config key", sut: ConfigKey("key", "value"), result: cfgerr},
		{name: "security protocol", sut: SecurityProtocol("protocol"), result: cfgerr},
		{name: "ssl ca location", sut: SslCaLocation("location"), result: cfgerr},
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
			got := Sasl("mechanisms", "username", "password")(cfg)

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
	cfg := func() Configuration { return func(cfg *Config) error { return cfgerr } }

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
	cfg := func() Configuration {
		return func(cfg *Config) error {
			if err := cfg.ConfigMap.SetKey("key", "value"); err != nil {
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
	wanted := &Config{ConfigMap: kafka.ConfigMap{"key": "value"}}
	if !maps.Equal(wanted.ConfigMap, got.ConfigMap) {
		t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
	}
}
