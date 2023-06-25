package kafka

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"syscall"
	"testing"
	"time"

	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"

	"github.com/blugnu/go-logspy"
	"github.com/blugnu/unilog4logrus"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/sirupsen/logrus"
)

func TestConsumer_commit(t *testing.T) {
	// ARRANGE
	ctx := context.Background()
	msg := &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: addr("topic"), Partition: 0, Offset: 1492}}

	committed := kafka.TopicPartition{}
	seeked := kafka.TopicPartition{}

	sut := &consumer{}
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

			// ACT
			err := sut.commit(ctx, msg, tc.readNext)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			// ASSERT
			t.Run("seeks", func(t *testing.T) {
				wanted := tc.seeks
				got := seeked.Offset
				if wanted != got {
					t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
				}
			})

			t.Run("commits", func(t *testing.T) {
				wanted := tc.commits
				got := committed.Offset
				if wanted != got {
					t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
				}
			})
		})
	}
}

func TestConsumer_commit_WhenCommitOffsetsReturnsErrors(t *testing.T) {
	// ARRANGE
	ctx := context.Background()
	msg := &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: addr("topic"), Partition: 0}}

	cmterr := errors.New("commit error")

	sut := &consumer{}
	sut.funcs.commitOffsets = func(offsets []kafka.TopicPartition) ([]kafka.TopicPartition, error) { return nil, cmterr }
	sut.funcs.seek = func(offset kafka.TopicPartition, _ int) error { return nil }

	// ACT
	got := sut.commit(ctx, msg, false)

	// ASSERT
	t.Run("returns error", func(t *testing.T) {
		wanted := cmterr
		if !errors.Is(got, wanted) {
			t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
		}

		t.Run("as fatal error ", func(t *testing.T) {
			wanted := FatalError{}
			if !errors.Is(got, wanted) {
				t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
			}
		})
	})
}

func TestConsumer_commit_WhenNoOffsetToCommit(t *testing.T) {
	// ARRANGE
	ctx := context.Background()
	msg := &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: addr("topic"), Partition: 0}}

	sut := &consumer{}
	sut.funcs.commitOffsets = func(offsets []kafka.TopicPartition) ([]kafka.TopicPartition, error) {
		return nil, kafka.NewError(kafka.ErrNoOffset, "no offset to commit", false)
	}

	// ACT
	got := sut.commit(ctx, msg, true) // readNext == true -> no need to mock funcs.seek

	// ASSERT
	t.Run("returns nil", func(t *testing.T) {
		if got != nil {
			t.Errorf("\nwanted nil\ngot    %T", got)
		}
	})
}

func TestConsumer_consumer(t *testing.T) {
	// ARRANGE
	ctx := context.Background()
	msg := &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: addr("topic"), Partition: 0, Offset: 10}}

	rerr := errors.New("readMessage error")
	herr := errors.New("handler error")
	serr := errors.New("seek error")
	cerr := errors.New("commit error")

	sut := &consumer{
		handlers: map[string]*handler{
			"topic": {
				EncryptionHandler:    NoEncryption(),
				RetryMetadataHandler: NoRetryMetadata(),
			},
		},
	}

	type result struct {
		seeks   kafka.Offset
		commits kafka.Offset
		error
	}
	const doesNotSeek = kafka.Offset(-1)
	const notCommitted = kafka.Offset(-1)

	testcases := []struct {
		name       string
		readerr    error
		handlerErr error
		seekErr    error
		commitErr  error
		result
	}{
		{name: "when readMessage fails", readerr: rerr, result: result{seeks: doesNotSeek, commits: notCommitted, error: rerr}},
		{name: "when handler returns nil", result: result{seeks: doesNotSeek, commits: msg.TopicPartition.Offset + 1}},
		{name: "when handler returns nil, commit fails", commitErr: cerr, result: result{seeks: doesNotSeek, commits: msg.TopicPartition.Offset + 1, error: FatalError{}}},
		{name: "when handler returns ErrReprocessMessage", handlerErr: ErrReprocessMessage, result: result{seeks: msg.TopicPartition.Offset, commits: msg.TopicPartition.Offset}},
		{name: "when handler returns ErrReprocessMessage, seek fails", handlerErr: ErrReprocessMessage, seekErr: serr, result: result{seeks: msg.TopicPartition.Offset, commits: notCommitted, error: FatalError{}}},
		{name: "when handler returns ErrReprocessMessage, commit fails", handlerErr: ErrReprocessMessage, commitErr: cerr, result: result{seeks: msg.TopicPartition.Offset, commits: msg.TopicPartition.Offset, error: FatalError{}}},
		{name: "when handler returns other error, commit ok", handlerErr: herr, result: result{seeks: doesNotSeek, commits: notCommitted, error: herr}},
		{name: "when handler returns other error, commit fails", handlerErr: herr, result: result{seeks: doesNotSeek, commits: notCommitted, error: herr}},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			// ARRANGE
			committed := kafka.Offset(notCommitted)
			seeked := kafka.Offset(-1)

			sut.funcs.readMessage = func(time.Duration) (*kafka.Message, error) {
				if tc.readerr != nil {
					return nil, tc.readerr
				}
				return msg, nil
			}
			sut.funcs.commitOffsets = func(offsets []kafka.TopicPartition) ([]kafka.TopicPartition, error) {
				committed = offsets[0].Offset
				if tc.commitErr != nil {
					return nil, tc.commitErr
				}
				return offsets, nil
			}
			sut.funcs.seek = func(offset kafka.TopicPartition, _ int) error {
				seeked = offset.Offset
				return tc.seekErr
			}

			sut.handlers["topic"].function = func(context.Context, *kafka.Message) error { return tc.handlerErr }
			sut.handlers["topic"].OnErrorHandler = &onerror{tc.handlerErr}

			// ACT
			err := sut.consume(ctx)

			// ASSERT
			t.Run("seeks", func(t *testing.T) {
				wanted := tc.result.seeks
				got := seeked
				if wanted != got {
					t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
				}
			})

			t.Run("commits", func(t *testing.T) {
				wanted := tc.result.commits
				got := committed
				if wanted != got {
					t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
				}
			})

			t.Run("error", func(t *testing.T) {
				wanted := tc.result.error
				got := err
				if !errors.Is(got, wanted) {
					t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
				}
			})
		})
	}
}

func TestConsumer_handleMessage(t *testing.T) {
	// ARRANGE
	ctx := context.Background()
	msg := &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: addr("topic"), Partition: 0, Offset: 10}}
	dcerr := errors.New("decrypt error")

	h := &handler{
		EncryptionHandler: &encryptionhandler{},
	}

	sut := &consumer{handlers: map[string]*handler{"topic": h}}

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
			msg.TopicPartition.Topic = addr(tc.topic)
			h.EncryptionHandler.(*encryptionhandler).decrypt = func(*kafka.Message) error { return tc.decryptErr }

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

func TestConsumer_handleMessage_AddsRetryMetadataToContext(t *testing.T) {
	// ARRANGE
	ctx := context.Background()
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: addr("topic")},
		Headers: []kafka.Header{
			{Key: "retry-seq", Value: []byte("1")},
			{Key: "retry-num", Value: []byte("2")},
			{Key: "retry-max", Value: []byte("3")},
			{Key: "retry-interval", Value: []byte("4s")},
			{Key: "retry-reason", Value: []byte("just because")},
		},
	}

	gotctx := ctx
	h := &handler{
		EncryptionHandler:    NoEncryption(),
		RetryMetadataHandler: DefaultRetryMetadataHandler(),
		function: func(ctx context.Context, msg *kafka.Message) error {
			gotctx = ctx
			return nil
		},
	}

	sut := &consumer{handlers: map[string]*handler{"topic": h}}

	// ACT
	err := sut.handleMessage(ctx, msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// ASSERT
	wanted := &RetryMetadata{Seq: 1, Num: 2, Max: 3, Interval: 4 * time.Second, Reason: "just because"}
	got := gotctx.Value(kRetryMetadata)
	if got == nil {
		t.Errorf("\nwanted %T\ngot    nil", wanted)
	}
}

func TestConsumer_readMessage_WhenTimeoutDetected(t *testing.T) {
	// ARRANGE
	ctx := context.Background()
	sut := &consumer{}

	sut.funcs.readMessage = func(time.Duration) (*kafka.Message, error) {
		return nil, kafka.NewError(kafka.ErrTimedOut, "fake timeout", false)
	}

	// ACT
	msg, err := sut.readMessage(ctx)

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
	sut := &consumer{stopped: make(chan bool, 1)}
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
	sut := &consumer{stopped: make(chan bool, 1)}
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
	wanted := csPanic
	got := sut.state
	if wanted != got {
		t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
	}
}

func TestConsumer_run_TerminatesOnCtrlC(t *testing.T) {
	// ARRANGE
	ctx := context.Background()
	sut := &consumer{
		ctrlc:   make(chan os.Signal, 1),
		stopped: make(chan bool, 1),
	}
	sut.funcs.close = func() error { return nil }

	signal.Notify(sut.ctrlc, os.Interrupt)
	sut.ctrlc <- syscall.Signal(2)

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
		ctrlc:   make(chan os.Signal, 1),
		stopped: make(chan bool, 1),
	}
	sut.funcs.close = func() error { return nil }
	sut.funcs.readMessage = func(time.Duration) (*kafka.Message, error) {
		return &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: addr("topic")}}, nil
	}

	sut.handlers = map[string]*handler{
		"topic": {
			function: func(context.Context, *kafka.Message) error { return errors.New("handler error") },
			EncryptionHandler: &encryptionhandler{
				decrypt: func(m *kafka.Message) error { return nil },
			},
			OnErrorHandler:       HaltConsumer(),
			RetryMetadataHandler: NoRetryMetadata(),
		},
	}

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
		t.Errorf("\nwanted %s\ngot    %s\n%s", wanted, got, sut.panic)
	}
}

func TestConsumer_run_ContinuesOnTimeout(t *testing.T) {
	// ARRANGE
	ctx := context.Background()
	sut := &consumer{
		ctrlc:   make(chan os.Signal, 1),
		stopped: make(chan bool, 1),
		handlers: map[string]*handler{
			"topic": {
				function:             func(context.Context, *kafka.Message) error { return nil },
				EncryptionHandler:    NoEncryption(),
				RetryMetadataHandler: NoRetryMetadata(),
			},
		},
	}
	signal.Notify(sut.ctrlc, os.Interrupt)

	msg := &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: addr("topic")}}

	msgs := 0
	timeout := false

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
			sut.ctrlc <- syscall.Signal(2)
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
	t.Run("terminated normally", func(t *testing.T) {
		wanted := csStopped
		got := sut.state
		if wanted != got {
			t.Errorf("\nwanted %s\ngot    %s, %s", wanted, got, sut.panic)
		}
	})

	t.Run("processed expected messages", func(t *testing.T) {
		wanted := 5
		got := msgs
		if wanted != got {
			t.Errorf("\nwanted %v\ngot    %v", wanted, got)
		}
	})
}

func TestConsumerStoppedChannelIsSignalledWhenConsumerHasStopped(t *testing.T) {
	// ARRANGE
	ctx := context.Background()

	sut := &consumer{
		ctrlc:   make(chan os.Signal, 1),
		stopped: make(chan bool, 1),
	}
	sut.funcs.close = func() error { return nil }
	sut.funcs.readMessage = func(time.Duration) (*kafka.Message, error) { return nil, ErrTimeout }

	// ACT
	func() {
		ctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel()

		go sut.run(ctx)

		<-sut.Stopped()
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
		handlers: map[string]*handler{
			"topic.1": {},
			"topic.2": {},
		},
	}
	suberr := errors.New("subscribe error")
	subscribed := []string{}
	sut.funcs.subscribeTopics = func(topics []string, rc kafka.RebalanceCb) error {
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
		ctrlc:   make(chan os.Signal, 1),
		stopped: make(chan bool, 1),
	}
	signal.Notify(sut.ctrlc, os.Interrupt)

	sut.funcs.close = func() error { return nil }
	sut.funcs.subscribeTopics = func([]string, kafka.RebalanceCb) error { return nil }

	runCalled := false
	sut.funcs.readMessage = func(time.Duration) (*kafka.Message, error) {
		runCalled = true
		sut.ctrlc <- syscall.Signal(2)
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
	ctx := context.Background()
	cfg := &Config{ConfigMap: kafka.ConfigMap{}}

	og := createConsumer
	defer func() { createConsumer = og }()

	createConsumer = func(cfg *kafka.ConfigMap) (*kafka.Consumer, error) {
		return &kafka.Consumer{}, nil
	}

	// ACT
	_, err := NewConsumer(ctx, cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// ASSERT
	wanted := kafka.ConfigMap{}
	got := cfg.ConfigMap
	if !maps.Equal(wanted, got) {
		t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
	}
}

func TestNewConsumerAddsDefaultConfiguration(t *testing.T) {
	// ARRANGE
	ctx := context.Background()
	cfg := &Config{ConfigMap: kafka.ConfigMap{}}

	og := createConsumer
	defer func() { createConsumer = og }()

	var cfgapplied kafka.ConfigMap
	createConsumer = func(cfg *kafka.ConfigMap) (*kafka.Consumer, error) {
		cfgapplied = *cfg
		return &kafka.Consumer{}, nil
	}

	// ACT
	_, err := NewConsumer(ctx, cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// ASSERT
	wanted := kafka.ConfigMap{
		"auto.offset.reset":      "earliest",
		"enable.auto.commit":     false,
		"go.logs.channel.enable": true,
	}
	got := cfgapplied
	if !maps.Equal(wanted, got) {
		t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
	}
}

func TestNewConsumerEnforcesRequiredConfiguration(t *testing.T) {
	// ARRANGE
	ctx := context.Background()
	cfg := &Config{ConfigMap: kafka.ConfigMap{
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
	_, err := NewConsumer(ctx, cfg, AutoOffsetReset(OffsetResetLatest))
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
	ctx := context.Background()
	cfg := &Config{ConfigMap: kafka.ConfigMap{}}
	cfgerr := errors.New("configuration error")

	opt := func(_ *Config, _ *consumer) error {
		return cfgerr
	}

	// ACT
	_, got := NewConsumer(ctx, cfg, opt)

	// ASSERT
	wanted := ConfigurationError{}
	if !errors.Is(got, wanted) {
		t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
	}
}

func TestNewConsumerReturnsConfigMapErrors(t *testing.T) {
	// ARRANGE
	ctx := context.Background()
	cfg := &Config{ConfigMap: kafka.ConfigMap{}}
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
			_, got := NewConsumer(ctx, cfg)

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

func TestNewConsumerAppliesDefaultsToHandlers(t *testing.T) {
	// ARRANGE
	ctx := context.Background()
	cfg := &Config{ConfigMap: kafka.ConfigMap{}}

	og := createConsumer
	defer func() { createConsumer = og }()
	createConsumer = func(cfg *kafka.ConfigMap) (*kafka.Consumer, error) {
		return &kafka.Consumer{}, nil
	}

	// ACT
	con, err := NewConsumer(ctx, cfg, Handlers(map[string]Handler{
		"configured": {
			Function:          func(ctx context.Context, m *Message) error { return nil },
			EncryptionHandler: &encryptionhandler{},
			OnError:           &onerror{},
		},
		"defaults": {
			Function: func(ctx context.Context, m *Message) error { return nil },
		},
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// ASSERT
	got := con.(*consumer)

	t.Run("applies Default.EncryptionHandler", func(t *testing.T) {
		wanted := Default.EncryptionHandler
		got := got.handlers["defaults"].EncryptionHandler
		if wanted != got {
			t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
		}
	})

	t.Run("applies Default.ConsumerErrorHandler", func(t *testing.T) {
		wanted := Default.ConsumerErrorHandler
		got := got.handlers["defaults"].OnErrorHandler
		if wanted != got {
			t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
		}
	})
}

func TestNewConsumerReturnsCreateConsumerErrors(t *testing.T) {
	// ARRANGE
	ctx := context.Background()
	cfg := &Config{ConfigMap: kafka.ConfigMap{}}
	createerr := errors.New("create error")

	og := createConsumer
	defer func() { createConsumer = og }()
	createConsumer = func(cfg *kafka.ConfigMap) (*kafka.Consumer, error) { return nil, createerr }

	// ACT
	_, got := NewConsumer(ctx, cfg)

	// ASSERT
	wanted := createerr
	if !errors.Is(got, wanted) {
		t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
	}
}

func TestConsumerLogsChannelGoRoutineTerminatesWhenChannelIsClosed(t *testing.T) {
	// ARRANGE
	ctx := context.Background()
	cfg := &Config{ConfigMap: kafka.ConfigMap{}}

	og := createConsumer
	defer func() { createConsumer = og }()
	createConsumer = func(cfg *kafka.ConfigMap) (*kafka.Consumer, error) { return &kafka.Consumer{}, nil }

	// ACT
	con, err := NewConsumer(ctx, cfg)
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
	wanted := true
	got := consumer.logsChannel == nil
	if wanted != got {
		t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
	}
}

func TestConsumerLogsCloseError(t *testing.T) {
	// ARRANGE
	ctx := context.Background()

	lr := logrus.New()
	lr.Formatter = &logrus.JSONFormatter{}
	lr.SetOutput(logspy.Sink())
	lr.SetLevel(logrus.InfoLevel)

	og := Logger
	defer func() {
		Logger = og
		logspy.Reset()
	}()
	Logger, _ = unilog4logrus.Logger(ctx, lr)

	sut := &consumer{
		ctrlc:   make(chan os.Signal, 1),
		stopped: make(chan bool, 1),
	}
	sut.funcs.close = func() error { return errors.New("logged close error") }

	signal.Notify(sut.ctrlc, os.Interrupt)
	sut.ctrlc <- syscall.Signal(2)

	// ACT
	go sut.run(ctx)
	<-sut.stopped

	// ASSERT
	if !logspy.Contains("logged close error") {
		t.Errorf("\nunexpected log content:\n%v", logspy.String())
	}
}
