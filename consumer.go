package kafka

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sync"
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
	IsRunning() error
	Start(ctx context.Context) error
	Wait() error
}

type consumer struct {
	groupId     string
	handlers    map[string]Handler
	readTimeout time.Duration
	sig         chan os.Signal
	logsChannel chan kafka.LogEvent
	waiting     []chan error
	err         error
	*kafka.Consumer
	cypher
	funcs struct {
		close         func() error
		commitOffsets func([]kafka.TopicPartition) ([]kafka.TopicPartition, error)
		readMessage   func(time.Duration) (*kafka.Message, error)
		seek          func(kafka.TopicPartition, int) error
		subscribe     func(topics []string, rebalanceCb kafka.RebalanceCb) error
	}
	state struct {
		sync.Mutex
		value consumerState
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
	const timeoutMS = 100

	offset := msg.TopicPartition
	if readNext {
		offset.Offset += 1
	} else if err := c.funcs.seek(offset, timeoutMS); err != nil {
		return fmt.Errorf("commit: seek: %w", err)
	}

	committed, err := c.funcs.commitOffsets([]kafka.TopicPartition{offset})
	if err != nil {
		if IsKafkaError(err, kafka.ErrNoOffset) {
			logs.Debug(ctx, "no offset to commit", LogInfo{Consumer: &c.groupId})
			return nil
		}
		return fmt.Errorf("commit: %w", err)
	}

	logs.Debug(ctx, "offset committed", LogInfo.withOffsetDetails(LogInfo{
		Consumer: &c.groupId,
	}, committed[0]),
	)

	return nil
}

// IsRunning returns an error if the consumer is not running.
//
// If the consumer is not running, the error will be one of:
//
//   - ErrInvalidOperation: the consumer is nil;
//
//   - ErrConsumerNotRunning: the consumer has not been started or has
//     stopped. The error will include the current state of the consumer;
//
//   - ErrConsumerError: the consumer has stopped due to an error; this
//     error is returned wrapped with any error on the consumer itself.
func (c *consumer) IsRunning() error {
	if c == nil {
		return fmt.Errorf("%w: consumer is nil", ErrInvalidOperation)
	}

	if state := c.getState(); state != csRunning {
		if c.err != nil {
			return fmt.Errorf("%w: %w", ErrConsumerError, c.err)
		}
		return fmt.Errorf("%w: current state: %s", ErrConsumerNotRunning, c.state.value)
	}
	return nil
}

// Start starts the consumer.  The consumer must be started before it can
// receive messages.
//
// If the consumer is already running, Start will return immediately with
// ErrInvalidOperation.
//
// Starting the consumer will subscribe to the topics for which handlers have
// been registered.  If no handlers have been registered, the consumer will
// return ErrNoHandlersConfigured.
//
// If the consumer is unable to subscribe to the topics, the error from the
// subscription request will be returned.
func (c *consumer) Start(ctx context.Context) error {
	if err := c.subscribe(ctx); err != nil {
		return err
	}

	go c.run(ctx)
	return nil
}

// Stop signals the consumer to stop processing messages.  Any messages
// currently being handled will be completed before the consumer stops, which
// may be after this function has returned.
//
// If the consumer has already stopped nil is returned without signalling
// the consumer to stop again.
//
// If the consumer has not been started ErrConsumerNotStarted is returned.
func (c *consumer) Stop() error {
	if c == nil {
		return fmt.Errorf("%w: consumer is nil", ErrInvalidOperation)
	}

	state := c.getState()

	if state > csRunning {
		return nil
	}
	if state < csRunning {
		return fmt.Errorf("stop: %w: current state: %s", ErrConsumerNotStarted, c.state.value)
	}

	c.sig <- syscall.SIGINT
	return nil
}

// Wait blocks until the consumer run loop has stopped.  A consumer
// may be stopped:
//
// - in response to a SIGINT or SIGTERM signal;
// - as a result of a message handler panicking or returning a kafka.FatalError;
// - as a result of an error in the consumer itself;
// - as a result of a call to the Stop method.
//
// Wait returns nil immediately if the consumer is nil.
//
// If Wait is called on a consumer that has not been started, ErrConsumerNotStarted
// is returned.
//
// If Wait is called on a consumer that has already stopped, the error that caused
// the consumer to stop is returned.
func (c *consumer) Wait() error {
	if c == nil {
		return nil
	}

	ch := make(chan error, 1)
	func() {
		state := c.getState()
		switch {
		case state < csSubscribed:
			// the consumer has not been started; send an appropriate error
			// to the channel immediately (channel will be closed)
			ch <- ErrConsumerNotStarted

		case state >= csStopped:
			// the consumer has already stopped; send any consumer error to
			// the channel immediately (channel will be closed)
			ch <- c.err

		default:
			// the consumer is running; add the channel to the waiting list
			// and return (leaving the channel open; it will be closed by the
			// consumer when the consumer stops)
			c.waiting = append(c.waiting, ch)
			return
		}

		// if we reach this point we have already sent any relevant error to
		// the channel and do not need to leave it open
		close(ch)
	}()

	// if the channel was closed above, the function will immediately return
	// with the error that was sent to the channel
	//
	// if the channel was added to the waiting list, the function will block
	// until the channel is closed by the consumer (when the consumer stops)
	// and then return any erro that caused the consumer to stop (or nil)
	return <-ch
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
//	any other error      -> no attempt is made to commit any offset;
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
	if errors.Is(err, ErrTimeout) {
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
	if reprocess := errors.Is(err, ErrReprocessMessage); reprocess || err == nil {
		if reprocess {
			// the original error is expected to have been logged by the handler;
			// additional debug logging is provided to indicate that the message
			// will be reprocessed
			logs.Debug(ctx, "message will be reprocessed",
				LogInfo.withOffsetDetails(
					LogInfo{Consumer: &c.groupId},
					msg.TopicPartition,
				),
			)
		}

		// commit the offset of the message; if the error was nil, the offset
		// will be committed to read the next message, otherwise to re-read
		// the same message
		readNext := err == nil
		if err := commitOffset(ctx, c, msg, readNext); err != nil {
			// if the commit fails, log the error and return it; this will
			// cause the consumer to stop processing messages
			logs.Error(ctx, "commit offset error",
				LogInfo.withOffsetDetails(
					LogInfo{
						Consumer: &c.groupId,
						Error:    err,
					},
					msg.TopicPartition,
				),
			)
			return err
		}

		// the error was nil or ErrReprocessMessage; if the latter then the reprocessing
		// has been organized and there was no error as far as the caller is concerned,
		// so we return nil to indicate that the message was handled successfully
		return nil
	}

	return err
}

// getState returns the state of the consumer.
func (c *consumer) getState() consumerState {
	c.state.Lock()
	defer c.state.Unlock()
	return c.state.value
}

// lockState locks the state and returns a pointer to the state value
// for the caller to work with.  The caller must call state.Unlock().
func (c *consumer) lockState() *consumerState {
	c.state.Lock()
	return &c.state.value
}

// setState updates the state of the consumer and returns the new state.
func (c *consumer) setState(state consumerState) consumerState {
	c.state.Lock()
	defer c.state.Unlock()
	c.state.value = state
	return state
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
		return handle(fmt.Errorf("decrypt message: %w", err))
	}

	// the handler is called in a function that will recover any panic
	// and log the details before re-panicking; this enables the log
	// entry to include details of the message being processed at the
	// time of the panic.
	//
	// the re-panic is necessary to ensure the consumer stops processing
	// messages and terminates in a panic state.
	err := func() error {
		defer func() {
			if r := recover(); r != nil {
				logs.Error(ctx, "handler panic", LogInfo.withMessageDetails(
					LogInfo{
						Consumer:  &c.groupId,
						Recovered: addr(r),
					}, msg),
				)
				panic(r)
			}
		}()
		return h.HandleMessage(ctx, msg)
	}()

	if err != nil && !errors.Is(err, ErrReprocessMessage) {
		logs.Error(ctx, "handler error", LogInfo.withMessageDetails(
			LogInfo{
				Consumer: &c.groupId,
				Error:    err,
			}, msg),
		)
	}

	return err
}

// readMessage reads the next message from the consumer or returns
// ErrTimeout if no message is available within the configured timeout.
func (c *consumer) readMessage() (*kafka.Message, error) {
	msg, err := c.funcs.readMessage(c.readTimeout)
	if IsKafkaError(err, kafka.ErrTimedOut) {
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

	exitLog := logs.Info // will be upgraded to logs.Error if an error occurs
	logInfo := LogInfo{
		Consumer: &c.groupId,
	}

	defer func() {
		c.cleanup(ctx, err, recover())
	}()

	c.setState(csRunning)
	logs.Info(ctx, "waiting for messages", logInfo)

runLoop:
	for {
		select {
		case <-ctx.Done():
			logInfo.Reason = addr("context done")
			break runLoop

		case <-c.sig:
			logInfo.Reason = addr("signal")
			break runLoop

		default:
			if err = c.consume(ctx); err == nil {
				continue
			}
			logInfo.Reason = addr("error")
			logInfo.Error = err
			exitLog = logs.Error
			break runLoop
		}
	}
	exitLog(ctx, "consumer stopping", logInfo)
	c.setState(csStopping)
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
	state := c.setState(map[bool]consumerState{
		true:  csPanic,
		false: csStopped,
	}[recovered != nil])

	if state == csPanic {
		err = &ConsumerPanicError{Recovered: recovered}
	}

	if closeErr := c.funcs.close(); closeErr != nil {
		logs.Error(ctx, "error closing consumer", LogInfo{
			Consumer: &c.groupId,
			Error:    closeErr,
		})
	}

	// if the consumer was stopped due to an error (or panic), set the error
	// on the consumer so it may be retrieved by the IsRunning() method
	c.err = err

	for _, ch := range c.waiting {
		ch <- err
		close(ch)
	}

	close(c.sig)

	logs.Info(ctx, "consumer stopped", LogInfo{
		Consumer: &c.groupId,
	})
}

// subscribe subscribes the consumer to the topics for which handlers have
// been registered.
//
// If no handlers have been registered, the consumer will return ErrNoHandlersConfigured.
func (c *consumer) subscribe(ctx context.Context) error {
	// this function changes the state of the consumer so we acquire a pointer
	// the state value from lockState to ensure the state is updated correctly
	state := c.lockState()
	defer c.state.Unlock()

	if *state != csReady {
		return fmt.Errorf("start: %w: current state: %s", ErrInvalidOperation, c.state.value)
	}

	if len(c.handlers) == 0 {
		return fmt.Errorf("start: %w", ErrNoHandlersConfigured)
	}

	topics := []string{}
	for t := range c.handlers {
		topics = append(topics, t)
	}

	logs.Debug(ctx, "subscribing to topics", LogInfo{
		Consumer: &c.groupId,
		Topics:   addr(topics),
	})

	if err := c.funcs.subscribe(topics, nil); err != nil {
		c.err = err
		*state = csError
		return fmt.Errorf("start: subscribe: %w", err)
	}

	*state = csSubscribed
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

// NewConsumer creates a new Kafka consumer with the specified configuration
// and options.  The consumer must be started before it can receive messages.
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

	consumer.setState(csReady)

	return consumer, nil
}
