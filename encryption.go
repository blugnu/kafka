package kafka

import "github.com/confluentinc/confluent-kafka-go/kafka"

// EncryptionHandler is an interface that groups Decrypt and
// Encrypt methods
type EncryptionHandler interface {
	Decrypt(*kafka.Message) error
	Encrypt(*kafka.Message) error
}

// encryptionhandler provides an implementation of EncryptionHandler
// using the provided functions
//
// this is used as the basis for predefined encryption handlers and
// also for encryption handlers created using the NewEncryptionHandler
// function, accepting individual Decrypt/Encrypt functions as an
// alternative to implementing the EncryptionHandler interface on a
// custom type
type encryptionhandler struct {
	decrypt func(*kafka.Message) error
	encrypt func(*kafka.Message) error
}

// Decrypt implements the DecryptMethod interface
func (handler *encryptionhandler) Decrypt(msg *kafka.Message) error { return handler.decrypt(msg) }

// Encrypt implements the EncryptMethod interface
func (handler *encryptionhandler) Encrypt(msg *kafka.Message) error { return handler.encrypt(msg) }

// DecryptFunc is the signature of a function that decrypts a message
type DecryptFunc func(*kafka.Message) error

// EncryptFunc is the signature of a function that encrypts a message
type EncryptFunc func(*kafka.Message) error

// NewEncryptionHandler creates a new EncryptionHandler using the
// provided Decrypt/Encrypt functions
//
// this provides an alternative to creating custom encryption handlers
// by implementing the EncryptionHandler interface on a custom type
func NewEncryptionHandler(decrypt DecryptFunc, encrypt EncryptFunc) EncryptionHandler {
	return &encryptionhandler{decrypt: decrypt, encrypt: encrypt}
}

// _noencryption is a predefined EncryptionHandler that does not
// perform any encryption/decryption
var _noencryption = NewEncryptionHandler(
	func(m *kafka.Message) error { return nil },
	func(m *kafka.Message) error { return nil },
)

// NoEncryption provides a built-in EncryptionHandler that does not
// perform any encryption/decryption
func NoEncryption() EncryptionHandler { return _noencryption }
