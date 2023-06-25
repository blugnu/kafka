package kafka

import (
	"errors"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/blugnu/go-errorcontext"
	"github.com/confluentinc/confluent-kafka-go/kafka"

	"github.com/blugnu/kafka/context"
)

var createConsumer = kafka.NewConsumer

type Consumer interface {
	Stopped() chan bool
	Start(context.Context) error
}

type consumer struct {
	groupId     string
	handlers    map[string]*handler
	readTimeout time.Duration
	state       consumerstate
	ctrlc       chan os.Signal
	logsChannel chan kafka.LogEvent
	stopped     chan bool
	panic       string
	*kafka.Consumer
	Producer
	funcs struct {
		close           func() error
		commitOffsets   func([]kafka.TopicPartition) ([]kafka.TopicPartition, error)
		readMessage     func(time.Duration) (*kafka.Message, error)
		seek            func(kafka.TopicPartition, int) error
		subscribeTopics func(topics []string, rebalanceCb kafka.RebalanceCb) error
	}
}

// commit commits the offset of the specified message.
//
// If readNext is true, the offset is committed as is (the consumer will read
// the next message from the offset partition).
//
// If readNext is false, the offset is committed minus one (the consumer will
// read the same message again from that partition).
//
// If the commit fails, a FatalError is returned (the consumer will stop
// processing messages)
func (c *consumer) commit(ctx context.Context, msg *kafka.Message, readNext bool) error {
	log := Logger.WithContext(ctx)

	offset := msg.TopicPartition
	if readNext {
		offset.Offset += 1
	} else if err := c.funcs.seek(offset, 100); err != nil {
		return FatalError{fmt.Errorf("commit: seek: %w", err)}
	}

	committed, err := c.funcs.commitOffsets([]kafka.TopicPartition{offset})
	if err != nil {
		if err, ok := err.(kafka.Error); ok && err.Code() == kafka.ErrNoOffset {
			log.Debug("no offset to commit")
			return nil
		}
		return FatalError{fmt.Errorf("error committing offset: %w", err)}
	}

	ctx = context.WithOffset(ctx, &committed[0])
	log.WithContext(ctx).
		Trace("committed offset")

	return nil
}

// consume reads a message from the consumer, handles it and commits
// the offset.
//
// The offset committed depends on the result of the message handling:
//
//	ErrReprocessMessage  -> commit offset minus one
//
//	nil (no error)       -> commit offset as is
//
//	any other error      -> no offset is committed (the consumer will
//	                        terminate and the message will be available
//	                        for processing by the restarted consumer or
//	                        another consumer in the same group)
func (c *consumer) consume(ctx context.Context) error {
	log := Logger.WithContext(ctx)

	// read the next message
	msg, err := c.readMessage(ctx)
	if err != nil {
		return err
	}

	ctx = context.WithMessageReceived(ctx, msg)

	log = log.WithContext(ctx)
	log.Info("message received")

	if err := c.handleMessage(ctx, msg); err != nil {
		if errors.Is(err, ErrReprocessMessage) {
			if err := c.commit(ctx, msg, false); err != nil {
				return err
			}
			return nil
		}
		return err
	}

	if err := c.commit(ctx, msg, true); err != nil {
		log.Warn("failed to commit offset")
		return err
	}

	return nil
}

// handleMessage identifies the handler for a specified message, decrypts
// the message and calls the handler function.
func (c *consumer) handleMessage(ctx context.Context, msg *kafka.Message) error {
	handle := func(err error, msg string) error {
		return errorcontext.Wrap(ctx, err, fmt.Sprintf("handleMessage: %s", msg))
	}

	h, ok := c.handlers[*msg.TopicPartition.Topic]
	if !ok {
		return ErrNoHandler
	}

	// TODO: delay processing
	// delayed, err := h.delayProcessing(ctx, msg)
	// if delayed {
	// 	return nil
	// }

	if err := h.Decrypt(msg); err != nil {
		return handle(err, "failed to decrypt message")
	}

	// get any retry metadata from the message
	// and add it to the context
	if retry := h.RetryMetadataHandler.Get(msg); retry != nil {
		ctx = context.WithValue(ctx, kRetryMetadata, retry)
	}

	err := h.function(ctx, msg)
	if err != nil {
		// if the handler explicitly returns ErrReprocessMessage,
		// the message will be reprocessed by the consumer and
		// isn't subject to any error handling
		if errors.Is(err, ErrReprocessMessage) {
			Logger.WithContext(ctx).Trace("message will be reprocessed")
			return err
		}
		return h.HandleError(ctx, msg, err)
	}

	return nil
}

// readMessage reads the next message from the consumer or returns
// ErrTimeout if no message is available within the configured timeout.
func (c *consumer) readMessage(ctx context.Context) (*kafka.Message, error) {
	//log := Logger.WithContext(ctx)

	msg, err := c.funcs.readMessage(c.readTimeout)
	if err, ok := err.(kafka.Error); ok && err.Code() == kafka.ErrTimedOut {
		return nil, ErrTimeout
	}
	if err != nil {
		return nil, err
	}
	return msg, nil
}

// run provides the main loop for the consumer.
func (c *consumer) run(ctx context.Context) {
	log := Logger.WithContext(ctx)

	defer func() {
		c.state = csStopped

		if r := recover(); r != nil {
			c.state = csPanic
			c.panic = fmt.Sprintf("%s", r)
			log.Error(c.panic)
		}

		c.stopped <- true
		if err := c.funcs.close(); err != nil {
			log.Errorf("error closing consumer: %s", err)
		}

		if c.ctrlc != nil {
			close(c.ctrlc)
		}
		if c.stopped != nil {
			close(c.stopped)
		}
	}()

	c.state = csRunning

forLoop:
	for {
		select {
		case <-ctx.Done():
			log.Info("consumer stopping: context cancellation")
			break forLoop

		case <-c.ctrlc:
			log.Info("consumer stopping at user request")
			break forLoop

		default:
			err := c.consume(ctx)
			if err == nil || errors.Is(err, ErrTimeout) {
				continue
			}
			log.Error(err)
			break forLoop
		}
	}
	c.state = csStopping
}

func (c *consumer) Stopped() chan bool {
	return c.stopped
}

func (c *consumer) Start(ctx context.Context) error {
	ctx = context.WithGroupId(ctx, c.groupId)
	log := Logger.WithContext(ctx)

	topics := []string{}
	for t := range c.handlers {
		topics = append(topics, t)
	}

	log.WithField("topics", topics).Debug("subscribing to topics")

	if err := c.funcs.subscribeTopics(topics, nil); err != nil {
		return fmt.Errorf("kafka.Consumer.Run: failed to subscribe to topics: %w", err)
	}

	c.state = csSubscribed

	go c.run(ctx)
	return nil
}

func NewConsumer(ctx context.Context, config *Config, opts ...ConsumerConfiguration) (Consumer, error) {
	handle := func(err error) (Consumer, error) {
		return nil, fmt.Errorf("kafka.NewConsumer: %w", err)
	}

	consumer := &consumer{
		handlers:    map[string]*handler{},
		readTimeout: 1 * time.Second,
	}

	// clone the base config and set defaults
	config = config.clone()

	// start at the earliest unread message by default (may be overridden by consumer configuration
	if err := config.setKey("auto.offset.reset", "earliest"); err != nil {
		return handle(ConfigurationError{err})
	}

	// apply consumer configuration options
	for _, opt := range opts {
		if err := opt(config, consumer); err != nil {
			return handle(ConfigurationError{err})
		}
	}
	// set/reset required configuration
	// - commits must be explicitly managed by this consumer implementation
	// - logs are sent to the logs channel to not pollute stderr
	if err := config.setKey("enable.auto.commit", false); err != nil {
		return handle(ConfigurationError{err})
	}
	if err := config.setKey("go.logs.channel.enable", true); err != nil {
		return handle(ConfigurationError{err})
	}

	// set encryption and onerror handler for any topic handlers
	// that have not been configured
	for _, handler := range consumer.handlers {
		handler.EncryptionHandler = coalesce(handler.EncryptionHandler, Default.EncryptionHandler)
		handler.OnErrorHandler = coalesce(handler.OnErrorHandler, Default.ConsumerErrorHandler)
	}

	// create the confluent consumer
	var err error
	if consumer.Consumer, err = createConsumer(&config.ConfigMap); err != nil {
		return handle(fmt.Errorf("failed to create consumer: %w", err))
	}
	consumer.funcs.close = consumer.Consumer.Close
	consumer.funcs.commitOffsets = consumer.Consumer.CommitOffsets
	consumer.funcs.readMessage = consumer.Consumer.ReadMessage
	consumer.funcs.seek = consumer.Consumer.Seek
	consumer.funcs.subscribeTopics = consumer.Consumer.SubscribeTopics

	// start a go routine to read events from the logs channel to prevent
	// it filling up;
	consumer.logsChannel = consumer.Logs()
	go func() {
		for {
			if _, ok := <-consumer.logsChannel; !ok {
				consumer.logsChannel = nil
				return
			}
		}
	}()

	// establish a ctrl-c signal channel for running in interactive console
	consumer.ctrlc = make(chan os.Signal, 1)
	signal.Notify(consumer.ctrlc, os.Interrupt)

	consumer.stopped = make(chan bool, 1)

	consumer.state = csReady

	return consumer, nil
}
