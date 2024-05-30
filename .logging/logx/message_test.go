package log

import (
	"reflect"
	"testing"
	"time"

	"github.com/blugnu/test"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func Test_message_String(t *testing.T) {
	str := "string"
	ptn := int32(1)
	off := int64(1492)
	ts := time.Date(2019, 1, 1, 0, 0, 0, 0, time.UTC)

	type fields struct {
		Offset    *offset
		Key       *string
		Headers   map[string]string
		Timestamp *time.Time
		Age       *string
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{"zero", fields{}, ""},
		{"offset", fields{Offset: &offset{&str, &ptn, &off}}, "offset=[string/1:1492]"},
		{"key", fields{Key: &str}, "key=string"},
		{"headers", fields{Headers: map[string]string{"key": "value"}}, "headers=map[key:value]"},
		{"timestamp", fields{Timestamp: &ts}, "timestamp={2019-01-01 00:00:00 +0000 UTC}"},
		{"age", fields{Age: &str}, "age=string"},
		{"key and age", fields{Key: &str, Age: &str}, "key=string, age=string"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := &message{
				Topic:     tt.fields.Offset,
				Key:       tt.fields.Key,
				Headers:   tt.fields.Headers,
				Timestamp: tt.fields.Timestamp,
				Age:       tt.fields.Age,
			}
			test.That(t, msg.String()).Equals(tt.want)
		})
	}
}

func TestMessage(t *testing.T) {
	key := "key"

	// timestamp and age must be fixed for tests to avoid being broken by the wallclock;
	// for the same reason, every message in the test cases must include this timestamp
	ts := time.Date(2019, 1, 1, 0, 0, 0, 0, time.UTC)
	age := "1s"

	og := now
	defer func() { now = og }()
	now = func() time.Time { return ts.Add(time.Second) }

	tests := []struct {
		name string
		msg  *kafka.Message
		want *message
	}{
		{"with timestamp (min)", &kafka.Message{Timestamp: ts}, &message{Timestamp: &ts, Age: &age}},
		{"with key (and timestamp)", &kafka.Message{Key: []byte(key), Timestamp: ts}, &message{Key: &key, Timestamp: &ts, Age: &age}},
		{"with headers (and timestamp)", &kafka.Message{Headers: []kafka.Header{{Key: "key", Value: []byte("value")}}, Timestamp: ts}, &message{Headers: map[string]string{"key": "value"}, Timestamp: &ts, Age: &age}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Message(tt.msg); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("\nwanted %v\ngot    %v", tt.want, got)
			}
		})
	}
}
