package kafka

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"os/signal"
	"slices"
	"syscall"
	"testing"
	"time"

	"github.com/blugnu/test"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type logEvent struct {
	string
	LogInfo
	level string
}

func (ev logEvent) String() string {
	return fmt.Sprintf("level=%s message=%s info={%v}", ev.level, ev.string, ev.LogInfo)
}

func TestConsumer_cleanup(t *testing.T) {
	// ARRANGE
	testcases := []struct {
		scenario string
		exec     func(t *testing.T)
	}{
		{scenario: "no error, no panic",
			exec: func(t *testing.T) {
				// ARRANGE
				ctx := context.Background()
				stopped := false
				sut := &consumer{
					Consumer: &kafka.Consumer{},
					sig:      make(chan os.Signal, 1),
					stopped:  make(chan error, 1),
				}
				sut.funcs.close = func() error { return nil }

				// ACT
				go sut.cleanup(ctx, nil, nil)

				ctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
				defer cancel()
				select {
				case <-sut.stopped:
					stopped = true
				case <-ctx.Done():
					stopped = false
				}

				// ASSERT
				test.IsTrue(t, stopped)
			},
		},
		{scenario: "error, no panic",
			exec: func(t *testing.T) {
				// ARRANGE
				ctx := context.Background()
				stopped := false
				sut := &consumer{
					Consumer: &kafka.Consumer{},
					sig:      make(chan os.Signal, 1),
					stopped:  make(chan error, 1),
				}
				sut.funcs.close = func() error { return errors.New("the error") }

				// ACT
				go sut.cleanup(ctx, nil, nil)

				ctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
				defer cancel()
				select {
				case <-sut.stopped:
					stopped = true
				case <-ctx.Done():
					stopped = false
				}

				// ASSERT
				test.IsTrue(t, stopped)
			},
		},
		{scenario: "no error, panic",
			exec: func(t *testing.T) {
				// ARRANGE
				ctx := context.Background()
				stopped := false
				sut := &consumer{
					Consumer: &kafka.Consumer{},
					sig:      make(chan os.Signal, 1),
					stopped:  make(chan error, 1),
				}
				recovered := "panic"
				sut.funcs.close = func() error { return nil }

				// ACT
				go sut.cleanup(ctx, nil, recovered)

				ctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
				defer cancel()
				select {
				case <-sut.stopped:
					stopped = true
				case <-ctx.Done():
					stopped = false
				}

				// ASSERT
				test.IsTrue(t, stopped)
			},
		},
	}
	for _, tc := range testcases {
		t.Run(tc.scenario, func(t *testing.T) {
			tc.exec(t)
		})
	}
}

func TestConsumer_commitOffset_seekFails(t *testing.T) {
	// ARRANGE
	ctx := context.Background()
	msg := &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: test.AddressOf("topic"), Partition: 0, Offset: 1492}}

	seekErr := errors.New("seek error")

	sut := &consumer{groupId: "consumer-id"}
	sut.funcs.seek = func(offset kafka.TopicPartition, _ int) error { return seekErr }

	// ACT
	err := commitOffset(ctx, sut, msg, false) // readNext = true does not involve seek

	// ASSERT
	test.Error(t, err).Is(seekErr)
}

func TestConsumer_commitOffset(t *testing.T) {
	// ARRANGE
	ctx := context.Background()
	msg := &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: test.AddressOf("topic"), Partition: 0, Offset: 1492}}

	committed := kafka.TopicPartition{}
	seeked := kafka.TopicPartition{}

	sut := &consumer{groupId: "consumer-id"}
	sut.funcs.commitOffsets = func(offsets []kafka.TopicPartition) ([]kafka.TopicPartition, error) {
		committed = offsets[0]
		return offsets, nil
	}
	sut.funcs.seek = func(offset kafka.TopicPartition, _ int) error {
		seeked = offset
		return nil
	}

	testcases := []struct {
		name     string
		readNext bool
		seeks    kafka.Offset
		commits  kafka.Offset
	}{
		{name: "readNext: false", readNext: false, seeks: msg.TopicPartition.Offset, commits: msg.TopicPartition.Offset},
		{name: "readNext: true", readNext: true, seeks: -1, commits: msg.TopicPartition.Offset + 1},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			// ARRANGE
			committed.Offset = -1
			seeked.Offset = -1

			type logEvent struct {
				string
				LogInfo
			}
			logEvents := []logEvent{}
			defer test.Using(&logs, Loggers{
				Debug: func(ctx context.Context, s string, i LogInfo) {
					logEvents = append(logEvents, logEvent{s, i})
				},
			})()

			// ACT
			err := commitOffset(ctx, sut, msg, tc.readNext)

			// ASSERT
			test.That(t, err).IsNil()
			test.That(t, seeked.Offset).Equals(tc.seeks)
			test.That(t, committed.Offset).Equals(tc.commits)
			test.That(t, logEvents).Equals([]logEvent{
				{
					"offset committed",
					LogInfo{
						Consumer: addr("consumer-id"),
						Offset: &kafka.TopicPartition{
							Topic:     msg.TopicPartition.Topic,
							Partition: msg.TopicPartition.Partition,
							Offset:    tc.commits,
						},
					},
				},
			})
		})
	}
}

func TestConsumer_commitOffset_WhenCommitOffsetsReturnsErrors(t *testing.T) {
	// ARRANGE
	ctx := context.Background()
	msg := &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: test.AddressOf("topic"), Partition: 0}}

	cmterr := errors.New("commit error")

	sut := &consumer{}
	sut.funcs.commitOffsets = func(offsets []kafka.TopicPartition) ([]kafka.TopicPartition, error) { return nil, cmterr }
	sut.funcs.seek = func(offset kafka.TopicPartition, _ int) error { return nil }

	// ACT
	err := commitOffset(ctx, sut, msg, false)

	// ASSERT
	test.Error(t, err).Is(cmterr)
}

func TestConsumer_commit_WhenNoOffsetToCommit(t *testing.T) {
	// ARRANGE
	ctx := context.Background()
	msg := &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: test.AddressOf("topic"), Partition: 0}}

	sut := &consumer{groupId: "consumer-id"}
	sut.funcs.commitOffsets = func(offsets []kafka.TopicPartition) ([]kafka.TopicPartition, error) {
		return nil, kafka.NewError(kafka.ErrNoOffset, "no offset to commit", false)
	}

	type logEvent struct {
		string
		LogInfo
	}
	logEvents := []logEvent{}
	defer test.Using(&logs, Loggers{
		Debug: func(ctx context.Context, s string, i LogInfo) {
			logEvents = append(logEvents, logEvent{s, i})
		},
	})()

	// ACT
	result := commitOffset(ctx, sut, msg, true) // readNext == true -> no need to mock funcs.seek

	// ASSERT
	test.That(t, result).IsNil()
	test.That(t, logEvents).Equals([]logEvent{
		{
			"no offset to commit",
			LogInfo{Consumer: addr("consumer-id")},
		},
	})
}

func TestConsumer_consume(t *testing.T) {
	// ARRANGE
	ctx := context.Background()
	msg := &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: test.AddressOf("topic"), Partition: 0, Offset: 10}}

	rerr := errors.New("readMessage error")
	herr := errors.New("handler error")
	cerr := errors.New("commit error")

	sut := &consumer{
		groupId: "consumer-id",
		handlers: map[string]Handler{
			"topic": HandlerFunc(nil),
		},
	}

	type result struct {
		readNext bool
		error
		logs      []logEvent
		committed bool
	}
	testcases := []struct {
		name       string
		readerr    error
		handlerErr error
		seekErr    error
		commitErr  error
		result
	}{
		{name: "when readMessage fails", readerr: rerr, result: result{error: rerr,
			logs: []logEvent{
				{"read message error", LogInfo{
					Consumer: addr("consumer-id"),
					Error:    rerr,
				}, "error"},
			}}},
		{name: "when handler returns nil, commit fails", commitErr: cerr, result: result{readNext: true, error: cerr, committed: true,
			logs: []logEvent{
				{"commit failed", LogInfo{
					Consumer: addr("consumer-id"),
					Message:  msg,
					Error:    cerr,
				}, "error"},
			}}},
		{name: "when handler returns ErrReprocessMessage", handlerErr: ErrReprocessMessage, result: result{readNext: false, committed: true,
			logs: []logEvent{
				{level: "debug",
					string: "message will be reprocessed",
					LogInfo: LogInfo{
						Consumer: addr("consumer-id"),
						Message: &kafka.Message{
							TopicPartition: kafka.TopicPartition{
								Topic:     msg.TopicPartition.Topic,
								Partition: msg.TopicPartition.Partition,
								Offset:    msg.TopicPartition.Offset,
							},
						},
					}},
			}},
		},
		{name: "when handler returns ErrReprocessMessage, commit fails", handlerErr: ErrReprocessMessage, commitErr: cerr, result: result{readNext: false, error: cerr, committed: true,
			logs: []logEvent{
				{level: "debug",
					string: "message will be reprocessed",
					LogInfo: LogInfo{
						Consumer: addr("consumer-id"),
						Message: &kafka.Message{
							TopicPartition: kafka.TopicPartition{
								Topic:     msg.TopicPartition.Topic,
								Partition: msg.TopicPartition.Partition,
								Offset:    msg.TopicPartition.Offset,
							},
						},
					}},
				{level: "error",
					string: "commit failed",
					LogInfo: LogInfo{
						Consumer: addr("consumer-id"),
						Message: &kafka.Message{
							TopicPartition: kafka.TopicPartition{
								Topic:     msg.TopicPartition.Topic,
								Partition: msg.TopicPartition.Partition,
								Offset:    msg.TopicPartition.Offset,
							},
						},
						Error: cerr,
					}},
			}}},
		{name: "when handler returns other error", handlerErr: herr, result: result{readNext: false, error: herr, committed: false,
			logs: []logEvent{
				{level: "error",
					string: "handler error",
					LogInfo: LogInfo{
						Consumer: addr("consumer-id"),
						Message: &kafka.Message{
							TopicPartition: kafka.TopicPartition{
								Topic:     msg.TopicPartition.Topic,
								Partition: msg.TopicPartition.Partition,
								Offset:    msg.TopicPartition.Offset,
							},
						},
						Error: herr,
					}},
			}}},
		{name: "when handler returns nil", result: result{readNext: true, committed: true}},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			// ARRANGE
			readNext := false
			committed := false

			sut.cypher = noEncryption
			sut.funcs.readMessage = func(time.Duration) (*kafka.Message, error) {
				if tc.readerr != nil {
					return nil, tc.readerr
				}
				return msg, nil
			}
			defer test.Using(&commitOffset, func(_ context.Context, _ *consumer, _ *kafka.Message, next bool) error {
				committed = true
				readNext = next
				return tc.commitErr
			})()

			sut.handlers["topic"] = HandlerFunc(func(context.Context, *Message) error { return tc.handlerErr })

			var logEntries []logEvent
			defer test.Using(&logs, Loggers{
				Debug: func(ctx context.Context, s string, i LogInfo) {
					logEntries = append(logEntries, logEvent{s, i, "debug"})
				},
				Error: func(ctx context.Context, s string, i LogInfo) {
					logEntries = append(logEntries, logEvent{s, i, "error"})
				},
			})()

			// ACT
			err := sut.consume(ctx)

			// ASSERT
			test.Error(t, err).Is(tc.result.error)
			test.That(t, readNext).Equals(tc.result.readNext)
			test.That(t, committed).Equals(tc.result.committed)
			test.That(t, logEntries).Equals(tc.result.logs)
		})
	}
}

func TestConsumer_handleMessage(t *testing.T) {
	// ARRANGE
	ctx := context.Background()
	msg := &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: test.AddressOf("topic"), Partition: 0, Offset: 10}}
	dcerr := errors.New("decrypt error")

	sut := &consumer{handlers: map[string]Handler{"topic": HandlerFunc(nil)}}

	testcases := []struct {
		name       string
		topic      string
		decryptErr error
		result     error
	}{
		{name: "no handler for topic", topic: "unknown", result: ErrNoHandler},
		{name: "decrypt error", topic: "topic", decryptErr: dcerr, result: dcerr},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			// ARRANGE
			msg.TopicPartition.Topic = test.AddressOf(tc.topic)
			defer test.Using(&sut.cypher.decrypt, func(context.Context, *Message) error { return tc.decryptErr })()

			// ACT
			got := sut.handleMessage(ctx, msg)

			// ASSERT
			wanted := tc.result
			if !errors.Is(got, wanted) {
				t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
			}
		})
	}
}

func TestConsumer_readMessage_WhenTimeoutDetected(t *testing.T) {
	// ARRANGE
	sut := &consumer{}

	sut.funcs.readMessage = func(time.Duration) (*kafka.Message, error) {
		return nil, kafka.NewError(kafka.ErrTimedOut, "fake timeout", false)
	}

	// ACT
	msg, err := sut.readMessage()

	// ASSERT
	t.Run("returns message", func(t *testing.T) {
		got := msg
		if got != nil {
			t.Errorf("\nwanted nil\ngot    %T", got)
		}
	})

	t.Run("returns error", func(t *testing.T) {
		wanted := ErrTimeout
		got := err
		if !errors.Is(got, wanted) {
			t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
		}
	})
}

func TestConsumer_run_TerminatesWhenContextIsCancelled(t *testing.T) {
	// ARRANGE
	ctx := context.Background()
	sut := &consumer{
		sig:     make(chan os.Signal, 1),
		stopped: make(chan error, 1),
	}
	sut.funcs.close = func() error { return nil }
	sut.funcs.readMessage = func(time.Duration) (*kafka.Message, error) { return nil, ErrTimeout }

	// ACT
	func() {
		ctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel()

		go sut.run(ctx)
		<-sut.stopped
	}()

	// ASSERT
	wanted := csStopped
	got := sut.state
	if wanted != got {
		t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
	}
}

func TestConsumer_run_TerminatesOnPanic(t *testing.T) {
	// ARRANGE
	ctx := context.Background()
	sut := &consumer{
		cypher:  noEncryption,
		sig:     make(chan os.Signal, 1),
		stopped: make(chan error, 1),
	}
	sut.funcs.close = func() error { return nil }
	sut.funcs.readMessage = func(time.Duration) (*kafka.Message, error) { panic("panic!") }

	// ACT
	func() {
		ctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel()

		go sut.run(ctx)
		<-sut.stopped
	}()

	// ASSERT
	test.That(t, sut.state).Equals(csPanic)
}

func TestConsumer_run_TerminatesOnCtrlC(t *testing.T) {
	// ARRANGE
	ctx := context.Background()
	sut := &consumer{
		cypher:  noEncryption,
		sig:     make(chan os.Signal, 1),
		stopped: make(chan error, 1),
	}
	sut.funcs.close = func() error { return nil }

	signal.Notify(sut.sig, os.Interrupt)
	sut.sig <- syscall.Signal(2)

	// ACT
	func() {
		ctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel()

		go sut.run(ctx)
		<-sut.stopped
	}()

	// ASSERT
	wanted := csStopped
	got := sut.state
	if wanted != got {
		t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
	}
}

func TestConsumer_run_TerminatesOnHandlerError(t *testing.T) {
	// ARRANGE
	ctx := context.Background()
	sut := &consumer{
		cypher:  noEncryption,
		sig:     make(chan os.Signal, 1),
		stopped: make(chan error, 1),
	}
	sut.funcs.close = func() error { return nil }
	sut.funcs.readMessage = func(time.Duration) (*kafka.Message, error) {
		return &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: test.AddressOf("topic")}}, nil
	}

	sut.handlers = map[string]Handler{
		"topic": HandlerFunc(func(context.Context, *kafka.Message) error { return errors.New("error") }),
	}

	// ACT
	func() {
		ctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel()

		go sut.run(ctx)
		<-sut.stopped
	}()

	// ASSERT
	test.That(t, sut.state).Equals(csStopped)
}

func TestConsumer_run_ContinuesOnTimeout(t *testing.T) {
	// ARRANGE
	ctx := context.Background()
	sut := &consumer{
		sig:     make(chan os.Signal, 1),
		stopped: make(chan error, 1),
		handlers: map[string]Handler{
			"topic": HandlerFunc(func(context.Context, *kafka.Message) error { return nil }),
		},
	}
	signal.Notify(sut.sig, os.Interrupt)

	msg := &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: test.AddressOf("topic")}}

	msgs := 0
	timeout := false

	sut.cypher = noEncryption
	sut.funcs.close = func() error { return nil }
	sut.funcs.seek = func(kafka.TopicPartition, int) error { return nil }
	sut.funcs.commitOffsets = func(tpa []kafka.TopicPartition) ([]kafka.TopicPartition, error) { return tpa, nil }
	sut.funcs.readMessage = func(time.Duration) (*kafka.Message, error) {
		timeout = !timeout
		if timeout {
			return nil, ErrTimeout
		}

		msgs++
		if msgs == 5 {
			sut.sig <- syscall.Signal(2)
			return nil, ErrTimeout
		}
		return msg, nil
	}

	// ACT
	func() {
		ctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel()

		go sut.run(ctx)
		<-sut.stopped
	}()

	// ASSERT
	test.That(t, sut.state).Equals(csStopped)
	test.That(t, msgs).Equals(5)
}

func TestConsumerStoppedChannelIsSignalledWhenConsumerHasStopped(t *testing.T) {
	// ARRANGE
	ctx := context.Background()

	sut := &consumer{
		sig:     make(chan os.Signal, 1),
		stopped: make(chan error, 1),
	}
	sut.funcs.close = func() error { return nil }
	sut.funcs.readMessage = func(time.Duration) (*kafka.Message, error) { return nil, ErrTimeout }

	// ACT
	func() {
		ctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel()

		go sut.run(ctx)

		<-sut.stopped
	}()

	// ASSERT
	wanted := true
	got := sut.state == csStopped
	if wanted != got {
		t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
	}
}

func TestConsumer_Start_SubscribesToTopics(t *testing.T) {
	// ARRANGE
	ctx := context.Background()
	sut := &consumer{
		handlers: map[string]Handler{
			"topic.1": HandlerFunc(nil),
			"topic.2": HandlerFunc(nil),
		},
	}
	suberr := errors.New("subscribe error")
	subscribed := []string{}
	sut.funcs.subscribe = func(topics []string, rc kafka.RebalanceCb) error {
		subscribed = topics
		return suberr
	}

	// ACT
	err := sut.Start(ctx)

	// ASSERT
	t.Run("topics", func(t *testing.T) {
		wanted := []string{"topic.1", "topic.2"}
		got := subscribed
		slices.Sort(wanted)
		slices.Sort(got)
		if !slices.Equal(wanted, got) {
			t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
		}
	})

	t.Run("error", func(t *testing.T) {
		wanted := suberr
		got := err
		if !errors.Is(got, wanted) {
			t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
		}
	})
}

func TestConsumer_Start_StartsTheRunLoop(t *testing.T) {
	// ARRANGE
	ctx := context.Background()
	sut := &consumer{
		sig:     make(chan os.Signal, 1),
		stopped: make(chan error, 1),
	}
	signal.Notify(sut.sig, os.Interrupt)

	sut.funcs.close = func() error { return nil }
	sut.funcs.subscribe = func([]string, kafka.RebalanceCb) error { return nil }

	runCalled := false
	sut.funcs.readMessage = func(time.Duration) (*kafka.Message, error) {
		runCalled = true
		sut.sig <- syscall.Signal(2)
		return nil, ErrTimeout
	}

	var err error

	// ACT
	func() {
		ctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel()

		err = sut.Start(ctx)
		<-sut.stopped
	}()

	// ASSERT
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	wanted := true
	got := runCalled
	if wanted != got {
		t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
	}
}

func TestNewConsumerClonesTheBaseConfig(t *testing.T) {
	// ARRANGE
	cfg := &Config{config: kafka.ConfigMap{}}

	og := createConsumer
	defer func() { createConsumer = og }()

	createConsumer = func(cfg *kafka.ConfigMap) (*kafka.Consumer, error) {
		return &kafka.Consumer{}, nil
	}

	// ACT
	_, err := NewConsumer(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// ASSERT
	wanted := kafka.ConfigMap{}
	got := cfg.config
	if !maps.Equal(wanted, got) {
		t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
	}
}

func TestNewConsumerAddsDefaultConfiguration(t *testing.T) {
	// ARRANGE
	cfg := &Config{config: kafka.ConfigMap{}}

	og := createConsumer
	defer func() { createConsumer = og }()

	var cfgapplied kafka.ConfigMap
	createConsumer = func(cfg *kafka.ConfigMap) (*kafka.Consumer, error) {
		cfgapplied = *cfg
		return &kafka.Consumer{}, nil
	}

	// ACT
	_, err := NewConsumer(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// ASSERT
	test.That(t, cfgapplied).Equals(kafka.ConfigMap{
		"auto.offset.reset":      "earliest",
		"enable.auto.commit":     false,
		"go.logs.channel.enable": true,
	})
}

func TestNewConsumerEnforcesRequiredConfiguration(t *testing.T) {
	// ARRANGE
	cfg := &Config{config: kafka.ConfigMap{
		"enable.auto.commit":     true,
		"go.logs.channel.enable": false,
	}}

	og := createConsumer
	defer func() { createConsumer = og }()

	var cfgapplied kafka.ConfigMap
	createConsumer = func(cfg *kafka.ConfigMap) (*kafka.Consumer, error) {
		cfgapplied = *cfg
		return &kafka.Consumer{}, nil
	}

	// ACT
	_, err := NewConsumer(cfg, AutoOffsetReset(OffsetResetLatest))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// ASSERT
	wanted := kafka.ConfigMap{
		"auto.offset.reset":      "latest",
		"enable.auto.commit":     false,
		"go.logs.channel.enable": true,
	}
	got := cfgapplied
	if !maps.Equal(wanted, got) {
		t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
	}
}

func TestNewConsumerReturnsConsumerConfigurationErrors(t *testing.T) {
	// ARRANGE
	cfg := &Config{config: kafka.ConfigMap{}}
	cfgerr := errors.New("configuration error")

	opt := func(_ *Config, _ *consumer) error {
		return cfgerr
	}

	// ACT
	_, got := NewConsumer(cfg, opt)

	// ASSERT
	wanted := ConfigurationError{}
	if !errors.Is(got, wanted) {
		t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
	}
}

func TestNewConsumerReturnsconfigErrors(t *testing.T) {
	// ARRANGE
	cfg := &Config{config: kafka.ConfigMap{}}
	cfgerr := errors.New("configuration error")

	og := setKeyFn
	defer func() { setKeyFn = og }()

	// ARRANGE
	testcases := []struct {
		entry string
	}{
		{entry: "auto.offset.reset"},
		{entry: "enable.auto.commit"},
		{entry: "go.logs.channel.enable"},
	}
	for _, tc := range testcases {
		t.Run(tc.entry, func(t *testing.T) {
			// ARRANGE
			setKeyFn = func(_ kafka.ConfigMap, key string, value any) error {
				if key == tc.entry {
					return cfgerr
				}
				return nil
			}

			// ACT
			_, got := NewConsumer(cfg)

			// ASSERT
			t.Run("error", func(t *testing.T) {
				wanted := cfgerr
				if !errors.Is(got, wanted) {
					t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
				}
				t.Run("is ConfigurationError", func(t *testing.T) {
					wanted := ConfigurationError{}
					if !errors.Is(got, wanted) {
						t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
					}
				})
			})
		})
	}
}

func TestNewConsumerReturnsCreateConsumerErrors(t *testing.T) {
	// ARRANGE
	cfg := &Config{config: kafka.ConfigMap{}}
	createerr := errors.New("create error")

	og := createConsumer
	defer func() { createConsumer = og }()
	createConsumer = func(cfg *kafka.ConfigMap) (*kafka.Consumer, error) { return nil, createerr }

	// ACT
	_, got := NewConsumer(cfg)

	// ASSERT
	wanted := createerr
	if !errors.Is(got, wanted) {
		t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
	}
}

func TestConsumerLogsChannelGoRoutineTerminatesWhenChannelIsClosed(t *testing.T) {
	// ARRANGE
	ctx := context.Background()
	cfg := &Config{config: kafka.ConfigMap{}}

	og := createConsumer
	defer func() { createConsumer = og }()
	createConsumer = func(cfg *kafka.ConfigMap) (*kafka.Consumer, error) { return &kafka.Consumer{}, nil }

	// ACT
	con, err := NewConsumer(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	consumer := con.(*consumer)
	consumer.logsChannel = make(chan kafka.LogEvent)
	func() {
		ctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel()

		close(consumer.logsChannel)

		for {
			select {
			case <-ctx.Done():
				return
			default:
				if consumer.logsChannel == nil {
					return
				}
				time.Sleep(10 * time.Millisecond)
			}
		}
	}()

	// ASSERT
	test.That(t, consumer.logsChannel).IsNil()
}

func TestConsumerWait(t *testing.T) {
	// ARRANGE
	conerr := errors.New("error")
	sut := &consumer{
		stopped: make(chan error, 1),
	}
	sut.stopped <- conerr

	// ACT
	err := sut.Wait()

	// ASSERT
	test.Error(t, err).Is(conerr)
}
