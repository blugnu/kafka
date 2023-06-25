package kafka

import (
	"encoding/json"
	"testing"

	"github.com/blugnu/go-logspy"
	"github.com/blugnu/unilog4logrus"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/sirupsen/logrus"

	"github.com/blugnu/kafka/context"
)

func TestLogEnrichment(t *testing.T) {
	// ARRANGE
	ctx := context.Background()

	lr := logrus.New()
	lr.Formatter = &logrus.JSONFormatter{}
	lr.SetOutput(logspy.Sink())
	lr.SetLevel(logrus.InfoLevel)

	logger, _ := unilog4logrus.Logger(ctx, lr)

	testcases := []struct {
		name     string
		ctx      context.Context
		validate func(any) bool
	}{
		{name: "empty context", ctx: ctx, validate: func(fields any) bool {
			return fields == nil
		}},
		{name: "group id", ctx: context.WithGroupId(ctx, "group"), validate: func(fields any) bool {
			_, ok := fields.(map[string]any)["group_id"]
			return ok
		}},
		{name: "message produced", ctx: context.WithMessageProduced(ctx, &kafka.Message{}), validate: func(fields any) bool {
			msg, ok := fields.(map[string]any)["message"].(map[string]any)
			if ok {
				_, ok = msg["produced"]
			}
			return ok
		}},
		{name: "message received", ctx: context.WithMessageReceived(ctx, &kafka.Message{}), validate: func(fields any) bool {
			msg, ok := fields.(map[string]any)["message"].(map[string]any)
			if ok {
				_, ok = msg["received"]
			}
			return ok
		}},
		{name: "offset", ctx: context.WithOffset(ctx, &kafka.TopicPartition{}), validate: func(fields any) bool {
			_, ok := fields.(map[string]any)["offset"]
			return ok
		}},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			// ARRANGE
			log := logger.WithContext(tc.ctx)
			defer logspy.Reset()

			// ACT
			log.Info("test")

			// ASSERT
			fields := map[string]any{}
			entry := logspy.Sink().Bytes()
			err := json.Unmarshal(entry, &fields)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			wanted := true
			got := tc.validate(fields["kafka"])
			if wanted != got {
				t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
			}
		})
	}
}
