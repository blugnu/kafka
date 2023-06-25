package kafka

import (
	"github.com/blugnu/kafka/context"
)

// handler is the internal configuration of a handler. It combines
// explicit configuration provided by a supplied Handler{} configuration
// with default values as required.
type handler struct {
	function HandlerFunc
	EncryptionHandler
	RetryMetadataHandler
	OnErrorHandler
}

// HandlerFunc is the function signature for a handler function.
type HandlerFunc func(context.Context, *Message) error

// Handler is the configuration for a handler.
type Handler struct {
	Function HandlerFunc
	OnError  OnErrorHandler
	EncryptionHandler
}
