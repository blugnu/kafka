package kafka

import (
	"context"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/blugnu/test"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func TestLogInfoAsString(t *testing.T) {
	// ARRANGE
	info := LogInfo{
		Consumer: addr("group"),
		Offset:   &kafka.TopicPartition{Topic: addr("topic"), Partition: 1, Offset: 2},
		Message: &kafka.Message{
			Key: []byte("key-value"),
			Headers: []kafka.Header{
				{Key: "key1", Value: []byte("value1")},
				{Key: "key2", Value: []byte("value2")},
			},
			TopicPartition: kafka.TopicPartition{
				Topic:     addr("topic"),
				Partition: 1,
				Offset:    2,
			},
			Value:     []byte("value"),
			Timestamp: time.Date(2010, 9, 8, 7, 6, 5, 0, time.UTC),
		},
		Error:     fmt.Errorf("error"),
		Reason:    addr("reason"),
		Recovered: addr(any("recovered")),
		Topic:     addr("topic"),
		Topics:    &[]string{"topic1", "topic2"},
	}

	// ACT
	result := info.String()

	// ASSERT
	expected := "consumer=group offset=topic/1:2 message={offset=topic/1:2 key=[key-value] headers={key1:[value1] key2:[value2]} timestamp=2010-09-08T07:06:05Z} error=\"error\" reason=reason recovered=\"recovered\" topic=topic topics=[topic1 topic2]"
	test.That(t, result).Equals(expected)
}

func TestDefaultLogging(t *testing.T) {
	// ARRANGE
	ctx := context.Background()

	// ACT
	stdout, stderr := test.CaptureOutput(t, func() {
		logs.Debug(ctx, "this should not be logged", LogInfo{})
		logs.Info(ctx, "this should not be logged", LogInfo{})
		logs.Error(ctx, "this should not be logged", LogInfo{})
	})

	// ASSERT
	stdout.IsEmpty()
	stderr.IsEmpty()

	// ARRANGE
	defer test.Using(&logs, Loggers{})()
	EnableLogs(nil)

	// ACT
	stdout, stderr = test.CaptureOutput(t, func() {
		logs.Debug(ctx, "this will be logged", LogInfo{})
		logs.Info(ctx, "this will be logged", LogInfo{})
		logs.Error(ctx, "this will be logged", LogInfo{})
	})

	// // ASSERT
	stdout.IsEmpty()
	stderr.Contains([]string{
		"KAFKA:DEBUG this will be logged",
		"KAFKA:INFO  this will be logged",
		"KAFKA:ERROR this will be logged",
	})

	// ARRANGE
	EnableLogs(&Loggers{
		Info: func(ctx context.Context, s string, i LogInfo) {
			log.Println("INFO", s, i)
		},
	})

	// ACT
	stdout, stderr = test.CaptureOutput(t, func() {
		logs.Debug(ctx, "will NOT be logged", LogInfo{})
		logs.Info(ctx, "will be logged", LogInfo{})
		logs.Error(ctx, "will NOT be logged", LogInfo{})
	})

	// // ASSERT
	stdout.IsEmpty()
	stderr.Contains([]string{
		"INFO will be logged",
	})
}
