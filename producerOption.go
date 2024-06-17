package kafka

import (
	"context"
	"time"
)

type ProducerOption func(*Config, *producer) error

type ProducerPipeline struct {
	BeforeProduction Handler
	AfterDelivery    func(context.Context, *Message, error)
}

// DeliveryTimeout configures the producer to wait a specified duration
// for a message to be delivered before failing.  If not configured, the
// default timeout is determined by the broker (typically 30 seconds).
//
// This configuration option works in conjunction with the MaxRetries
// option.
//
// # DeliveryTimeout and MaxRetries
//
// If a delivery timeout occurs, the producer will retry producing the
// message up to the configured number of MaxRetries.  A timeout error
// will only be returned by a producer if a message is not delivered
// within the specified timeout period on the initial production and
// each retry attempt.
//
// e.g. DeliveryTimeout -> 1 second, MaxRetries -> 2 will return a timeout
// error after 3 seconds (1 second + 2 retries).
func DeliveryTimeout(timeout time.Duration) ProducerOption {
	return func(cfg *Config, _ *producer) error {
		return cfg.setKey("delivery.timeout.ms", int(timeout.Milliseconds()))
	}
}

// MaxRetries configures the producer to retry producing messages a specified
// number of times before failing.
//
// The producer will only retry messages that fail to produce due to a
// timeout.  All other delivery errors are returned immediately with no
// retries.
//
// The default number of retries is 2 (3 attempts in total).
func MaxRetries(retries int) ProducerOption {
	return func(cfg *Config, p *producer) error {
		p.maxRetries = retries
		return nil
	}
}

// MessageEncryption configures the producer to encrypt messages before
// producing them, using a specified function.
//
// Messages are encrypted after any configured message pipeline but before
// being produced.
func MessageEncryption(fn CypherFunc) ProducerOption {
	return func(cfg *Config, _ *producer) error {
		cfg.cypher = cypher{
			decrypt: cfg.decrypt,
			encrypt: fn,
		}
		return nil
	}
}

// MessagePipeline configures the producer to use a specified pipeline
// for processing messages before production and after delivery, consisting
// of two handlers:
//
//	BeforeProduction   // called before a message is produced; this handler
//	                   // is called before any encryption handler;
//
//	AfterDelivery      // called after a message has been delivered (or
//	                   // delivery has failed).
//
// The BeforeProduction handler is called with the message to be produced.
// If the BeforeProduction handler returns an error, the message is not
// produced.
//
// The AfterDelivery handler is called with the message that was produced and
// the delivery error, if any.
//
// The MessagePipeline is intended to be used to introduce cross-cutting
// concerns into message production, such as logging and trace instrumentation.
// Message encryption should be configured using the MessageEncryption option.
func MessagePipeline(pipeline ProducerPipeline) ProducerOption {
	return func(_ *Config, p *producer) error {
		if pipeline.BeforeProduction != nil {
			p.funcs.prepareMessage = pipeline.BeforeProduction.HandleMessage
		}
		p.funcs.checkDelivery = pipeline.AfterDelivery
		return nil
	}
}
