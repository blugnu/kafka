package kafka

import (
	"context"
	"errors"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var createProducer = kafka.NewProducer

type Producer interface {
	MustProduce(context.Context, *Message) (*kafka.TopicPartition, error)
}

type producer struct {
	*kafka.Producer
	EncryptionHandler
	logsChannel chan kafka.LogEvent
	funcs       struct {
		produce func(*kafka.Message, chan kafka.Event) error
	}
}

// checkDeliveryEvent checks a delivery event and returns the message
// and error if any.
//
// If the event is not a message or error, an UnexpectedDeliveryEvent
// error is returned.
func checkDeliveryEvent(ev kafka.Event) (*kafka.TopicPartition, error) {
	switch ev := ev.(type) {
	case *kafka.Message:
		return &ev.TopicPartition, ev.TopicPartition.Error
	case kafka.Error:
		return nil, ev
	}
	return nil, UnexpectedDeliveryEvent{ev}
}

func (p *producer) MustProduce(ctx context.Context, msg *Message) (*kafka.TopicPartition, error) {
	handle := func(err error) (*kafka.TopicPartition, error) {
		return nil, fmt.Errorf("MustProduce: %w", err)
	}

	dc := make(chan kafka.Event)

	if err := p.Encrypt(msg); err != nil {
		return handle(fmt.Errorf("encryption failed: %w", err))
	}

	if err := p.funcs.produce(msg, dc); err != nil {
		return handle(fmt.Errorf("producer error: %w", err))
	}

	tp, err := checkDeliveryEvent(<-dc)
	if err != nil {
		return handle(fmt.Errorf("delivery error: %w", err))
	}

	return tp, nil
}

func NewProducer(ctx context.Context, config *Config, opts ...ProducerConfiguration) (Producer, error) {
	handle := func(err error) (Producer, error) {
		return nil, fmt.Errorf("NewProducer: %w", err)
	}
	producer := &producer{}

	config = config.clone()
	errs := []error{}
	for _, opt := range opts {
		if err := opt(config, producer); err != nil {
			errs = append(errs, err)
		}
	}
	if err := errors.Join(errs...); err != nil {
		return handle(ConfigurationError{err})
	}

	if err := config.setKey("enable.idempotence", true); err != nil {
		return handle(ConfigurationError{err})
	}
	if err := config.setKey("go.logs.channel.enable", true); err != nil {
		return handle(ConfigurationError{err})
	}

	var err error
	producer.Producer, err = createProducer(&config.ConfigMap)
	if err != nil {
		return nil, fmt.Errorf("kafka.NewProducer: %w", err)
	}

	producer.funcs.produce = producer.Producer.Produce

	producer.logsChannel = producer.Logs()
	go func() {
		for {
			if _, ok := <-producer.logsChannel; !ok {
				producer.logsChannel = nil
				return
			}
		}
	}()

	return producer, nil
}
