package kafka

import (
	"errors"
	"fmt"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"golang.org/x/exp/maps"
)

var setKeyFn = func(cfg kafka.ConfigMap, key string, value any) error { return cfg.SetKey(key, value) }

type Configuration func(*Config) error

type Config struct {
	kafka.ConfigMap
}

func (cfg *Config) clone() *Config {
	return &Config{
		ConfigMap: maps.Clone(cfg.ConfigMap),
	}
}

func (cfg *Config) setKey(key string, value any) error {
	return setKeyFn(cfg.ConfigMap, key, value)
}

func BootstrapServers(servers ...string) Configuration {
	return func(cfg *Config) error {
		return cfg.setKey("bootstrap.servers", strings.Join(servers, ","))
	}
}

func Broker(name string, port int) Configuration {
	return func(cfg *Config) error {
		return cfg.setKey("bootstrap.servers", fmt.Sprintf("%s:%d", name, port))
	}
}

func ConfigKey(key string, value any) Configuration {
	return func(cfg *Config) error {
		return cfg.setKey(key, value)
	}
}

func Sasl(mechanisms, username, password string) Configuration {
	return func(cfg *Config) error {
		errs := []error{}
		errs = append(errs, cfg.setKey("sasl.mechanisms", mechanisms))
		errs = append(errs, cfg.setKey("sasl.username", username))
		errs = append(errs, cfg.setKey("sasl.password", password))
		return errors.Join(errs...)
	}
}

func SecurityProtocol(protocol string) Configuration {
	return func(cfg *Config) error {
		return cfg.setKey("security.protocol", protocol)
	}
}

func SslCaLocation(filepath string) Configuration {
	return func(cfg *Config) error {
		return cfg.setKey("ssl.ca.location", filepath)
	}
}

func NewConfig(opts ...Configuration) (*Config, error) {
	handle := func(err error, s string) (*Config, error) {
		return nil, fmt.Errorf("kafka.NewConfig: %s: %w", s, err)
	}

	cfg := &Config{
		ConfigMap: kafka.ConfigMap{},
	}
	for _, opt := range opts {
		if err := opt(cfg); err != nil {
			return handle(err, "configuration error")
		}
	}

	return cfg, nil
}
