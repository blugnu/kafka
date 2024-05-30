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

func DeliveryTimeout(timeout time.Duration) ProducerOption {
	return func(cfg *Config, _ *producer) error {
		return cfg.setKey("delivery.timeout.ms", int(timeout.Milliseconds()))
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
// If the handler returns an error, the message is not produced.
//
// The AfterDelivery handler is called with the message that was produced and
// the delivery error, if any.
func MessagePipeline(pipeline ProducerPipeline) ProducerOption {
	return func(_ *Config, p *producer) error {
		if pipeline.BeforeProduction != nil {
			p.funcs.prepareMessage = pipeline.BeforeProduction.HandleMessage
		}
		p.funcs.checkDelivery = pipeline.AfterDelivery
		return nil
	}
}
