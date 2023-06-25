package kafka

import (
	"reflect"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func Test_retrymetadatahandler_Get(t *testing.T) {
	sut := &retrymetadatahandler{}

	type args struct {
		msg *kafka.Message
	}
	tests := []struct {
		name string
		args args
		want *RetryMetadata
	}{
		{"nil", args{msg: nil}, nil},
		{"empty", args{msg: &kafka.Message{}}, nil},
		{"no retry headers", args{msg: &kafka.Message{Headers: []kafka.Header{{Key: "not-a-retry-header"}}}}, nil},
		{"seq", args{msg: &kafka.Message{Headers: []kafka.Header{{Key: "retry-seq", Value: []byte(`1`)}}}}, &RetryMetadata{Seq: 1}},
		{"num", args{msg: &kafka.Message{Headers: []kafka.Header{{Key: "retry-num", Value: []byte(`1`)}}}}, &RetryMetadata{Num: 1}},
		{"max", args{msg: &kafka.Message{Headers: []kafka.Header{{Key: "retry-max", Value: []byte(`1`)}}}}, &RetryMetadata{Max: 1}},
		{"interval", args{msg: &kafka.Message{Headers: []kafka.Header{{Key: "retry-interval", Value: []byte(`1s`)}}}}, &RetryMetadata{Interval: 1 * time.Second}},
		{"reason", args{msg: &kafka.Message{Headers: []kafka.Header{{Key: "retry-reason", Value: []byte(`error`)}}}}, &RetryMetadata{Reason: "error"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := sut.Get(tt.args.msg); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("retrymetadatahandler.Get() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_retrymetadatahandler_Set(t *testing.T) {
	sut := &retrymetadatahandler{}

	type args struct {
		md *RetryMetadata
	}
	tests := []struct {
		name string
		args args
		want []kafka.Header
	}{
		{"nil", args{md: nil}, nil},
		{"empty", args{md: &RetryMetadata{}}, nil},
		{"seq", args{md: &RetryMetadata{Seq: 1}}, []kafka.Header{{Key: "retry-seq", Value: []byte(`1`)}}},
		{"num", args{md: &RetryMetadata{Num: 1}}, []kafka.Header{{Key: "retry-num", Value: []byte(`1`)}}},
		{"max", args{md: &RetryMetadata{Max: 1}}, []kafka.Header{{Key: "retry-max", Value: []byte(`1`)}}},
		{"interval", args{md: &RetryMetadata{Interval: 1 * time.Second}}, []kafka.Header{{Key: "retry-interval", Value: []byte(`1s`)}}},
		{"reason", args{md: &RetryMetadata{Reason: "error"}}, []kafka.Header{{Key: "retry-reason", Value: []byte(`error`)}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := &kafka.Message{}
			sut.Set(msg, tt.args.md)

			got := msg.Headers
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("retrymetadatahandler.Set() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDefaultRetryMetadataHandler(t *testing.T) {
	tests := []struct {
		name string
		want RetryMetadataHandler
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := DefaultRetryMetadataHandler(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DefaultRetryMetadataHandler() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_noretrymetadata_Set(t *testing.T) {
	// ARRANGE
	sut := &noretrymetadata{}

	defer func() { // panic tests must be deferred
		if r := recover(); r == nil {
			t.Run("panics", func(t *testing.T) {
				t.Errorf("did not panic")
			})
		}
	}()

	// ACT
	sut.Set(nil, nil)
}
