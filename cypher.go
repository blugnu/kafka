package kafka

import (
	"context"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// CypherHandler is an interface that must be implemented by a handler provided to
// a Config for the encryption and decryption of messages.
type CypherHandler interface {
	Decrypt(ctx context.Context, msg *kafka.Message) error
	Encrypt(ctx context.Context, msg *kafka.Message) error
}

// CypherFunc is the signature for a function that can be used to encrypt or decrypt
// a message, as required by the kafka.MessageDecryption or kafka.MessageEncryption
// Consumer and Producer options.
type CypherFunc func(context.Context, *kafka.Message) error

// cypher is a struct that holds the encryption and decryption functions for a
// Config, Consumer or Producer. It captures the functions to be used for encryption
// and decryption of messages.
type cypher struct {
	decrypt CypherFunc
	encrypt CypherFunc
}

// noEncryption is a cypher that does not perform any encryption or decryption.
//
// This is used as a default cypher when no encryption or decryption is configured.
var noEncryption = cypher{
	decrypt: func(_ context.Context, _ *kafka.Message) error { return nil },
	encrypt: func(_ context.Context, _ *kafka.Message) error { return nil },
}
