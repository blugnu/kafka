package kafka

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/blugnu/errorcontext"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// replaceable functions for testing
var (
	createConsumer = kafka.NewConsumer
)

type Consumer interface {
	Start(context.Context) error
	Wait() error
}

type consumer struct {
	groupId     string
	handlers    map[string]Handler
	readTimeout time.Duration
	state       consumerState
	sig         chan os.Signal
	logsChannel chan kafka.LogEvent
	stopped     chan error
	*kafka.Consumer
	cypher
	funcs struct {
		close         func() error
		commitOffsets func([]kafka.TopicPartition) ([]kafka.TopicPartition, error)
		readMessage   func(time.Duration) (*kafka.Message, error)
		seek          func(kafka.TopicPartition, int) error
		subscribe     func(topics []string, rebalanceCb kafka.RebalanceCb) error
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
var commitOffset = func(ctx context.Context, c *consumer, msg *kafka.Message, readNext bool) error {
	offset := msg.TopicPartition
	if readNext {
		offset.Offset += 1
	} else if err := c.funcs.seek(offset, 100); err != nil {
		return fmt.Errorf("commit: seek: %w", err)
	}

	committed, err := c.funcs.commitOffsets([]kafka.TopicPartition{offset})
	if err != nil {
		if err, ok := err.(kafka.Error); ok && err.Code() == kafka.ErrNoOffset {
			logs.Debug(ctx, "no offset to commit", LogInfo{Consumer: &c.groupId})
			return nil
		}
		return fmt.Errorf("commit: %w", err)
	}

	logs.Debug(ctx, "offset committed", LogInfo{
		Consumer: &c.groupId,
		Offset:   &committed[0],
	})

	return nil
}

// consume reads a message from the consumer, handles it and commits
// the offset.
//
// The offset committed depends on the result of the message handling:
//
//	ErrReprocessMessage  -> commit offset minus one (the message will be
//	                        reprocessed)
//
//	nil (no error)       -> commit offset as-is (the message is processed)
//
//	any other error      -> no attempt is made to to commit any offset;
//	                        the consumer will terminate and the message will be
//	                        available for processing by the restarted consumer or
//	                        another consumer in the same group, after rebalancing)
func (c *consumer) consume(ctx context.Context) error {
	// NOTE: error logging is performed here, even though any error is also
	// returned by the method.
	//
	// REASON: The caller is the consumer run loop which will not have access to
	// details of the message to add to the logs; the run loop uses the returned
	// error to determine whether or not to continue processing messages and to
	// record the error in the log at that .

	// read the next message
	msg, err := c.readMessage()
	if err == ErrTimeout {
		return nil
	}
	if err != nil {
		logs.Error(ctx, "read message error", LogInfo{
			Consumer: &c.groupId,
			Error:    err,
		})
		return err
	}

	err = c.handleMessage(ctx, msg)
	if err == nil || err == ErrReprocessMessage {
		if err == ErrReprocessMessage {
			// the original error is expected to have been logged by the handler
			logs.Debug(ctx, "message will be reprocessed", LogInfo{
				Consumer: &c.groupId,
				Message:  msg,
			})
		}
		if err := commitOffset(ctx, c, msg, err == nil); err != nil {
			logs.Error(ctx, "commit failed", LogInfo{
				Consumer: &c.groupId,
				Message:  msg,
				Error:    err,
			})
			return err
		}
		return nil
	}

	logs.Error(ctx, "handler error", LogInfo{
		Consumer: &c.groupId,
		Message:  msg,
		Error:    err,
	})

	return err
}

// handleMessage identifies the handler for a specified message, decrypts
// the message and calls the handler function.
func (c *consumer) handleMessage(ctx context.Context, msg *kafka.Message) error {
	handle := func(err error) error {
		return errorcontext.Errorf(ctx, "handleMessage: %w", err)
	}

	h, ok := c.handlers[*msg.TopicPartition.Topic]
	if !ok {
		return ErrNoHandler
	}

	if err := c.decrypt(ctx, msg); err != nil {
		return handle(fmt.Errorf("decrypt: %w", err))
	}

	return h.HandleMessage(ctx, msg)
}

// readMessage reads the next message from the consumer or returns
// ErrTimeout if no message is available within the configured timeout.
func (c *consumer) readMessage() (*kafka.Message, error) {
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
	var err error

	defer func() {
		c.cleanup(ctx, err, recover())
	}()

	c.state = csRunning

	exitInfo := LogInfo{
		Consumer: &c.groupId,
	}
	exitLog := logs.Info // upgraded to logs.Error if an error occurs

	logs.Info(ctx, "waiting for messages", exitInfo)

runLoop:
	for {
		select {
		case <-ctx.Done():
			exitInfo.Reason = addr("context done")
			break runLoop

		case <-c.sig:
			exitInfo.Reason = addr("signal")
			break runLoop

		default:
			if err = c.consume(ctx); err == nil {
				continue
			}
			exitInfo.Reason = addr("error")
			exitInfo.Error = err
			exitLog = logs.Error
			break runLoop
		}
	}
	exitLog(ctx, "consumer stopping", exitInfo)

	c.state = csStopping
}

// cleanup performs the necessary cleanup operations when the consumer
// is stopped. The function is called when the run() func exits, with:
//
//   - EITHER the error that caused the run loop to terminate;
//   - OR a recovered value from a panic;
//   - OR nil error AND nil panic.
//
// The function will never be called with BOTH a non-nil error AND a
// recovered panic value.
func (c *consumer) cleanup(ctx context.Context, err error, recovered any) {
	c.state = map[bool]consumerState{
		true:  csPanic,
		false: csStopped,
	}[recovered != nil]

	if c.state == csPanic {
		logs.Error(ctx, "consumer panic", LogInfo{
			Consumer:  &c.groupId,
			Recovered: addr(recovered),
		})
		err = &ConsumerPanic{Recovered: recovered}
	}

	if closeErr := c.funcs.close(); closeErr != nil {
		logs.Error(ctx, "error closing consumer", LogInfo{
			Consumer: &c.groupId,
			Error:    closeErr,
		})
	}

	// send the error to the stopped channel so it may be collected by any
	// goroutine that may be waiting for the consumer
	c.stopped <- err

	close(c.sig)
	close(c.stopped)

	logs.Info(ctx, "consumer stopped", LogInfo{
		Consumer: &c.groupId,
	})
}

// Wait blocks until the consumer run loop has stopped.  A consumer
// may be stopped as a result of a message handler panicking or returning
// a kafka.FatalError or in response to a SIGINT or SIGTERM signal.
func (c *consumer) Wait() error {
	return <-c.stopped
}

func (c *consumer) Start(ctx context.Context) error {
	topics := []string{}
	for t := range c.handlers {
		topics = append(topics, t)
	}

	logs.Debug(ctx, "subscribing to topics", LogInfo{
		Consumer: &c.groupId,
		Topics:   addr(topics),
	})

	if err := c.funcs.subscribe(topics, nil); err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}

	c.state = csSubscribed

	go c.run(ctx)
	return nil
}

func (c *consumer) configure(cfg *Config, opts ...ConsumerOption) error {
	handle := func(err error) error {
		return fmt.Errorf("configure: %w", err)
	}

	// start at the earliest unread message by default (may be overridden
	// by consumer configuration
	if err := cfg.setKey("auto.offset.reset", "earliest"); err != nil {
		return handle(ConfigurationError{err})
	}

	// apply consumer configuration options
	for _, opt := range opts {
		if err := opt(cfg, c); err != nil {
			return handle(ConfigurationError{err})
		}
	}
	// set/reset required configuration
	// - commits must be explicitly managed by this consumer implementation
	// - logs are sent to the logs channel to not pollute stderr
	if err := cfg.setKey("enable.auto.commit", false); err != nil {
		return handle(ConfigurationError{err})
	}
	if err := cfg.setKey("go.logs.channel.enable", true); err != nil {
		return handle(ConfigurationError{err})
	}

	// if no cypher is configured use the noEncryption NO-OP cypher
	if c.cypher = cfg.cypher; c.decrypt == nil {
		c.cypher = noEncryption
	}

	return nil
}

func NewConsumer(config *Config, opts ...ConsumerOption) (Consumer, error) {
	handle := func(err error) (Consumer, error) {
		return nil, fmt.Errorf("kafka.NewConsumer: %w", err)
	}

	consumer := &consumer{
		handlers:    map[string]Handler{},
		readTimeout: 1 * time.Second,
	}

	// clone the config and apply options
	config = config.clone()
	if err := consumer.configure(config, opts...); err != nil {
		return handle(err)
	}

	// create the confluent consumer
	var err error
	if consumer.Consumer, err = createConsumer(&config.config); err != nil {
		return handle(fmt.Errorf("createConsumer: %w", err))
	}
	consumer.funcs.close = consumer.Consumer.Close
	consumer.funcs.commitOffsets = consumer.Consumer.CommitOffsets
	consumer.funcs.readMessage = consumer.Consumer.ReadMessage
	consumer.funcs.seek = consumer.Consumer.Seek
	consumer.funcs.subscribe = consumer.Consumer.SubscribeTopics

	// start a go routine to read events from the logs channel to prevent
	// it filling up;
	consumer.logsChannel = consumer.Consumer.Logs()
	go func() {
		for {
			if _, ok := <-consumer.logsChannel; !ok {
				consumer.logsChannel = nil
				return
			}
		}
	}()

	// establish a ctrl-c signal channel for running in interactive console
	consumer.sig = make(chan os.Signal, 1)
	signal.Notify(consumer.sig, syscall.SIGINT, syscall.SIGTERM)

	consumer.stopped = make(chan error, 1)
	consumer.state = csReady

	return consumer, nil
}
