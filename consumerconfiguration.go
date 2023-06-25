package kafka

import (
	"fmt"
	"time"
)

type ConsumerConfiguration func(*Config, *consumer) error

func AutoOffsetReset(opt OffsetReset) ConsumerConfiguration {
	return func(cfg *Config, _ *consumer) error {
		return cfg.setKey("auto.offset.reset", string(opt))
	}
}

func GroupId(id string) ConsumerConfiguration {
	return func(cfg *Config, consumer *consumer) error {
		if err := cfg.setKey("group.id", id); err != nil {
			return err
		}
		consumer.groupId = id
		return nil
	}
}

func Handlers[T comparable](handlers map[T]Handler) ConsumerConfiguration {
	return func(_ *Config, c *consumer) error {
		for topic, hcfg := range handlers {
			if hcfg.Function == nil {
				return fmt.Errorf("handler for topic %v has no function: %w", topic, ErrHandlerFunctionIsRequired)
			}

			handler := &handler{
				function:             hcfg.Function,
				EncryptionHandler:    coalesce(hcfg.EncryptionHandler, Default.EncryptionHandler),
				OnErrorHandler:       coalesce(hcfg.OnError, Default.ConsumerErrorHandler),
				RetryMetadataHandler: NoRetryMetadata(),
			}
			c.handlers[fmt.Sprintf("%v", topic)] = handler

			if retry, ok := handler.OnErrorHandler.(*Retry[T]); ok {
				rt := coalesce(retry.Topic, topic)
				c.handlers[fmt.Sprintf("%v", rt)] = handler

				retry.consumer = c
				retry.MetadataHandler = coalesce(retry.MetadataHandler, DefaultRetryMetadataHandler())
				retry.Throttling = coalesce(retry.Throttling, UnthrottledRetries())

				handler.RetryMetadataHandler = coalesce(retry.MetadataHandler, Default.RetryMetadataHandler)
			}

		}
		return nil
	}
}

func ReadTimeout(timeout time.Duration) ConsumerConfiguration {
	return func(cfg *Config, consumer *consumer) error {
		if timeout < 0 && timeout != -1 {
			return ErrInvalidReadTimeout
		}
		consumer.readTimeout = timeout
		return nil
	}
}

// WithProducer configures the producer to be used by a consumer when
// producing internal messages (i.e. retries).  If not specified, the
// consumer will use the Default.Producer.
func WithProducer(producer Producer) ConsumerConfiguration {
	return func(cfg *Config, consumer *consumer) error {
		consumer.Producer = producer
		return nil
	}
}
