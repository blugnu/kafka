package kafka

import (
	"context"
	"errors"
	"fmt"

	"github.com/blugnu/kafka/internal/mock"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// replaceable functions for testing
var createProducer = kafka.NewProducer

type Producer interface {
	MustProduce(context.Context, *kafka.Message) (*kafka.TopicPartition, error)
}

type MockProducer[T comparable] interface {
	Expect(T) *mock.Expectation
	ExpectationsWereMet() error
	Reset()
}

type producer struct {
	*kafka.Producer
	logsChannel chan kafka.LogEvent
	cypher
	funcs struct {
		checkDelivery  func(context.Context, *Message, error)
		prepareMessage func(context.Context, *Message) error
		produce        func(*kafka.Message, chan kafka.Event) error
	}
}

// checkDeliveryEvent checks a delivery event and returns the message
// and error if any.
//
// If the event is not a message or error, an UnexpectedDeliveryEvent
// error is returned.
var checkDeliveryEvent = func(ev kafka.Event) (*kafka.TopicPartition, error) {
	switch ev := ev.(type) {
	case *kafka.Message:
		return &ev.TopicPartition, ev.TopicPartition.Error
	case kafka.Error:
		return nil, ev
	}
	return nil, UnexpectedDeliveryEvent{ev}
}

func (p *producer) configure(config *Config, opts ...ProducerOption) error {
	handle := func(err error) error {
		return fmt.Errorf("configure: %w", err)
	}

	// default configuration which may be overridden
	if err := config.setKey("delivery.timeout.ms", 5000); err != nil {
		return handle(ConfigurationError{err})
	}

	errs := []error{}
	for _, opt := range opts {
		if err := opt(config, p); err != nil {
			errs = append(errs, err)
		}
	}
	if err := errors.Join(errs...); err != nil {
		return handle(ConfigurationError{err})
	}

	// ensure that required configuration is applied, potentially
	// overriding any user-provided configuration
	if err := config.setKey("enable.idempotence", true); err != nil {
		return handle(ConfigurationError{err})
	}
	if err := config.setKey("go.logs.channel.enable", true); err != nil {
		return handle(ConfigurationError{err})
	}

	// if no cypher is configured use the noEncryption NO-OP cypher
	if p.cypher = config.cypher; p.encrypt == nil {
		p.cypher = noEncryption
	}

	if p.funcs.prepareMessage == nil {
		p.funcs.prepareMessage = func(context.Context, *Message) error { return nil }
	}
	if p.funcs.checkDelivery == nil {
		p.funcs.checkDelivery = func(context.Context, *Message, error) { /* NO-OP */ }
	}

	return nil
}

func (p *producer) MustProduce(ctx context.Context, msg *Message) (*kafka.TopicPartition, error) {
	handle := func(err error) (*kafka.TopicPartition, error) {
		logs.Error(ctx, "error producing message", LogInfo{
			Topic: msg.TopicPartition.Topic,
			Error: err,
		})

		return nil, fmt.Errorf("MustProduce: %w", err)
	}

	dc := make(chan kafka.Event)
	defer close(dc)

	if err := p.funcs.prepareMessage(ctx, msg); err != nil {
		return handle(fmt.Errorf("prepare: %w", err))
	}

	if err := p.encrypt(ctx, msg); err != nil {
		return handle(fmt.Errorf("encrypt: %w", err))
	}

	if err := p.funcs.produce(msg, dc); err != nil {
		return handle(fmt.Errorf("produce: %w", err))
	}

	tp, err := checkDeliveryEvent(<-dc)
	if p.funcs.checkDelivery(ctx, msg, err); err != nil {
		return handle(fmt.Errorf("delivery: %w", err))
	}

	logs.Info(ctx, "message produced", LogInfo{Topic: tp.Topic})

	return tp, nil
}

func NewMockProducer[T comparable](ctx context.Context) (Producer, MockProducer[T]) {
	p := &mock.Producer[T]{}
	return p, p
}

func NewProducer(ctx context.Context, config *Config, opts ...ProducerOption) (Producer, error) {
	config = config.clone()

	p := &producer{}
	if err := p.configure(config, opts...); err != nil {
		return nil, fmt.Errorf("kafka.NewProducer: %w", err)
	}

	var err error
	p.Producer, err = createProducer(&config.config)
	if err != nil {
		return nil, fmt.Errorf("kafka.NewProducer: %w", err)
	}

	p.funcs.produce = p.Producer.Produce

	p.logsChannel = p.Producer.Logs()
	go func() {
		for {
			if _, ok := <-p.logsChannel; !ok {
				p.logsChannel = nil
				return
			}
		}
	}()

	return p, nil
}
