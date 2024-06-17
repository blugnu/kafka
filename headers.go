package kafka

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// headers is a type that is cast-compatible with the headers of a
// Kafka message and provides methods for querying the headers.
type headers []kafka.Header

// Get returns the value of the specified key.
// If the key is not present in the headers, nil is returned.
func (h headers) Get(key string) []byte {
	for _, header := range h {
		if header.Key == key {
			return header.Value
		}
	}
	return nil
}

// GetString returns the value of the specified key as a string.
// If the key is not present in the headers, an empty string is returned.
func (h headers) GetString(key string) string {
	if v := h.Get(key); v != nil {
		return string(v)
	}
	return ""
}

// HasKey returns true if the specified key is present in the headers.
func (h headers) HasKey(key string) bool {
	for _, header := range h {
		if header.Key == key {
			return true
		}
	}
	return false
}

// Headers returns the headers of the specified message type-cast
// to a more convenient type for working with headers.
//
// The returned value supports methods for querying the headers:
//
//   - Get(key string) []byte
//   - GetString(key string) string
//   - HasKey(key string) bool
func Headers(msg *kafka.Message) headers {
	return headers(msg.Headers)
}
