package kafka

import (
	"errors"
	"fmt"
	"maps"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

var setKeyFn = func(cfg kafka.ConfigMap, key string, value any) error { return cfg.SetKey(key, value) }

type ConfigOption func(*Config) error

type Config struct {
	config kafka.ConfigMap
	cypher
}

// NewConfig creates a new Config and applies the specified options.
func NewConfig(opts ...ConfigOption) (*Config, error) {
	cfg := &Config{
		config: kafka.ConfigMap{},
	}
	for _, opt := range opts {
		if err := opt(cfg); err != nil {
			return nil, fmt.Errorf("kafka.NewConfig: %w", err)
		}
	}

	return cfg, nil
}

// clone returns a deep copy of the Config.
func (cfg *Config) clone() *Config {
	return &Config{
		config: maps.Clone(cfg.config),
		cypher: cfg.cypher,
	}
}

// setKey sets a key-value pair in the ConfigMap contained by the Config.
func (cfg *Config) setKey(key string, value any) error {
	return setKeyFn(cfg.config, key, value)
}

// BootstrapServers sets the Kafka broker addresses.
//
// Sets the 'bootstrap.servers' key in the config map.
//
// The addresses should be a comma-separated list of host:port pairs.
func BootstrapServers(servers ...string) ConfigOption {
	return func(cfg *Config) error {
		return cfg.setKey("bootstrap.servers", strings.Join(servers, ","))
	}
}

// Broker provides an alternative way to set a broker host name and address.
//
// Sets the 'bootstrap.servers' key in the config map.
//
// Broker() is equivalent to using BootstrapServers() with a single server and
// will overwrite any existing value for 'bootstrap.servers'.
func Broker(name string, port int) ConfigOption {
	return func(cfg *Config) error {
		return cfg.setKey("bootstrap.servers", fmt.Sprintf("%s:%d", name, port))
	}
}

// ConfigKey sets a key-value pair in the ConfigMap contained by the Config.
//
// This option may be used to set any key-value pair in the ConfigMap for which
// there is no specific option function provided.
func ConfigKey(key string, value any) ConfigOption {
	return func(cfg *Config) error {
		return cfg.setKey(key, value)
	}
}

// Cypher identifies a handler for both encrypting and decrypting messages.
//
// This option provides configuration for the Consumer and Producer implementation
// provided by this package; it does not affect the config map.
//
// The handler must implement the CypherHandler interface:
//
//	type CypherHandler interface {
//		Decrypt(context.Context, *kafka.Message) error
//		Encrypt(context.Context, *kafka.Message) error
//	}
//
// A consumer created with this config will use the Cypher.Decrypt method to
// decrypt messages before passing them to the appropriate topic handler.
//
// A producer created with this config will use the Cypher.Encrypt method to
// encrypt messages before sending them to the Kafka broker.
//
// # Separate Configuration
//
// Consumer and Producer specific options can also be used to configure encryption
// and decryption separately:
//
//	kafka.NewConsumer(ctx, cfg, kafka.MessageDecryption(decryptFn))
//	kafka.NewProducer(ctx, cfg, kafka.MessageEncryption(encryptFn))
//
// Where decryptFn and encryptFn are functions with the signature required by
// the CypherHandler interface methods: func(context.Context, *kafka.Message) error
//
// This may be preferred if an application only incorporates a Consumer or Producer,
// but not both or where an application produces different messages with differing
// encryption needs:
//
//	pBlue, err := kafka.NewProducer(ctx, cfg, kafka.MessageEncryption(blue))
//	pGreen, err := kafka.NewProducer(ctx, cfg, kafka.MessageEncryption(green))
func Cypher(handler CypherHandler) ConfigOption {
	return func(cfg *Config) error {
		cfg.decrypt = handler.Decrypt
		cfg.encrypt = handler.Encrypt
		return nil
	}
}

// SASL sets the SASL mechanisms, username, and password for the Kafka client.
//
// Sets the following keys in the config map:
//
// - sasl.mechanisms
// - sasl.username
// - sasl.password
//
// The mechanisms should be a comma-separated list of mechanisms to use for
// authentication.  The username and password are the credentials to use for
// authentication.
func SASL(mechanisms, username, password string) ConfigOption {
	return func(cfg *Config) error {
		errs := []error{}
		errs = append(errs, cfg.setKey("sasl.mechanisms", mechanisms))
		errs = append(errs, cfg.setKey("sasl.username", username))
		errs = append(errs, cfg.setKey("sasl.password", password))
		return errors.Join(errs...)
	}
}

// SecurityProtocol sets the security protocol for the Kafka client.
//
// Sets the 'security.protocol' key in the config map.
//
// The protocol should be one of the following values:
//
// - PLAINTEXT
// - SSL
// - SASL_PLAINTEXT
// - SASL_SSL
func SecurityProtocol(protocol string) ConfigOption {
	return func(cfg *Config) error {
		return cfg.setKey("security.protocol", protocol)
	}
}

// SSLCALocation sets the file path to the CA certificate for SSL connections.
//
// Sets the 'ssl.ca.location' key in the config map.
func SSLCALocation(filepath string) ConfigOption {
	return func(cfg *Config) error {
		return cfg.setKey("ssl.ca.location", filepath)
	}
}
