package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/blugnu/kafka/internal/mock"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// replaceable functions for testing
var createProducer = kafka.NewProducer

type Producer interface {
	Err() error
	MustProduce(ctx context.Context, msg kafka.Message) (*TopicPartition, error)
}

type MockProducer[T comparable] interface {
	Expect(topic T) *mock.Expectation
	ExpectationsWereMet() error
	Reset()
}

const (
	defaultMaxRetries        = 2
	defaultDeliveryTimeoutMS = 5000 // in milliseconds
)

type producer struct {
	sync.Mutex
	*kafka.Producer
	cypher
	logsChannel chan kafka.LogEvent
	maxRetries  int
	err         error
	funcs       struct {
		checkDelivery  func(context.Context, *Message, error)
		prepareMessage func(context.Context, *Message) error
		produce        func(*kafka.Message, chan kafka.Event) error
	}
}

// NewProducer creates a new Producer with the given configuration and options.
func NewProducer(ctx context.Context, config *Config, opts ...ProducerOption) (Producer, error) {
	config = config.clone()

	p := &producer{
		maxRetries: defaultMaxRetries,
	}
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

// NewMockProducer creates a new mock producer that can be used in tests.  The function
// returns a Producer interface to be injected into dependents that require a Producer,
// and a MockProducer interface, used by a test to set and test expectations.
func NewMockProducer[T comparable](ctx context.Context) (Producer, MockProducer[T]) {
	p := &mock.Producer[T]{}
	return p, p
}

// deliveryEvent takes a delivery event and returns the delivered offset
// of the message if successfully delivered (or nil) and any error.
//
// If the event is not a message or error, an UnexpectedDeliveryEvent
// error is returned.
var deliveryEvent = func(ev kafka.Event) (*kafka.TopicPartition, error) {
	switch ev := ev.(type) {
	case *kafka.Message:
		return &ev.TopicPartition, ev.TopicPartition.Error
	case kafka.Error:
		return nil, ev
	}
	return nil, UnexpectedDeliveryEvent{ev}
}

// Err returns the last error that occurred when producing a message.  If no error
// has occurred, nil is returned.  Any error is cleared after a successful call to
// Produce.
//
// Err is safe to call concurrently.
//
// If the producer is nil, ErrInvalidOperation is returned.
//
// The function is intended to be used in concurrent health checks.  For example a
// K8s liveness HTTP probe endpoint might call this function.
func (p *producer) Err() error {
	if p == nil {
		return fmt.Errorf("%w: producer is nil", ErrInvalidOperation)
	}

	p.Lock()
	defer p.Unlock()

	return p.err
}

// MustProduce produces a message and returns the topic partition if successful, or an error.
//
// If the producer is nil, ErrInvalidOperation is returned.
//
// If an error occurs, the error is stored in the producer as well as returned to the caller.
// The stored error is intended to be used in concurrent health checks using the Err() method.
// Any stored error is cleared only after a subsequent successful call to Produce (or replaced
// by a later error).
//
// If a message timeout error occurs, the producer will retry producing the message up to the
// configured maximum number of retries.  If the maximum number of retries is reached, ErrTimeout
// is returned and stored for retrieval by Err().  A timeout error will not replace any current
// error already stored on the producer until the retry limit has been reached.
func (p *producer) MustProduce(ctx context.Context, msg Message) (*TopicPartition, error) {
	handle := func(err error) (*TopicPartition, error) {
		p.Lock()
		defer p.Unlock()

		p.err = err

		logs.Error(ctx, "error producing message", LogInfo{
			Topic: msg.TopicPartition.Topic,
			Error: err,
		})

		return nil, fmt.Errorf("MustProduce: %w", err)
	}

	dc := make(chan kafka.Event)
	defer close(dc)
	{
		msg := msg

		if err := p.funcs.prepareMessage(ctx, &msg); err != nil {
			return handle(fmt.Errorf("prepare: %w", err))
		}

		if err := p.encrypt(ctx, &msg); err != nil {
			return handle(fmt.Errorf("encrypt: %w", err))
		}

		var (
			tp    *kafka.TopicPartition
			err   error
			retry int
		)
		for { // timeout retries
			if err = p.funcs.produce(&msg, dc); err != nil {
				return handle(fmt.Errorf("produce: %w", err))
			}

			tp, err = deliveryEvent(<-dc)
			if IsKafkaError(err, kafka.ErrMsgTimedOut) {
				if retry++; retry > p.maxRetries {
					return nil, ErrTimeout
				}
				logs.Debug(ctx, "delivery timeout; retrying", LogInfo{Topic: msg.TopicPartition.Topic})
				continue
			}
			break
		}
		if p.funcs.checkDelivery(ctx, &msg, err); err != nil {
			return handle(fmt.Errorf("delivery: %w", err))
		}
		p.err = nil

		logs.Info(ctx, "message produced", LogInfo.
			withMessageDetails(LogInfo{}, &msg).
			withOffsetDetails(*tp),
		)
		return tp, nil
	}
}

func (p *producer) configure(config *Config, opts ...ProducerOption) error {
	handle := func(err error) error {
		return fmt.Errorf("configure: %w", err)
	}

	// default configuration which may be overridden
	if err := config.setKey("delivery.timeout.ms", defaultDeliveryTimeoutMS); err != nil {
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
