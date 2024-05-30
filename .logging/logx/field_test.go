package log

import (
	"reflect"
	"testing"
	"time"

	"github.com/blugnu/kafka/context"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func TestField(t *testing.T) {
	ctx := context.Background()
	group := "group"
	top := "topic"
	ptn := int32(1)
	os := int64(1492)
	ts := time.Now()
	age := time.Second
	agestr := "1s"

	off := kafka.TopicPartition{Topic: &top, Partition: 1, Offset: 1492}
	msg := &kafka.Message{TopicPartition: off, Timestamp: ts}
	mr := &messages{Received: &message{Topic: &offset{&top, &ptn, &os}, Timestamp: &ts, Age: &agestr}}
	mp := &messages{Produced: &message{Topic: &offset{&top, &ptn, &os}, Timestamp: &ts}}

	og := now
	defer func() { now = og }()
	now = func() time.Time { return ts.Add(+age) }

	tests := []struct {
		name string
		ctx  context.Context
		want *field
	}{
		{"empty", ctx, nil},
		{"group id", context.WithGroupId(ctx, group), &field{GroupId: &group}},
		{"message received", context.WithMessageReceived(ctx, msg), &field{Message: mr}},
		{"message produced", context.WithMessageProduced(ctx, msg), &field{Message: mp}},
		{"offset", context.WithOffset(ctx, &off), &field{Offset: &offset{&top, &ptn, &os}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Field(tt.ctx); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("\nwanted %v\ngot    %v", tt.want, got)
			}
		})
	}
}

// func Test_field_String(t *testing.T) {
// 	top := "topic"
// 	ptn := int32(1)
// 	off := int64(1492)

// 	type fields struct {
// 		GroupId *string
// 		Message *messages
// 		Offset  *offset
// 	}
// 	tests := []struct {
// 		name   string
// 		fields fields
// 		want   string
// 	}{
// 		{"empty", fields{}, ""},
// 		{"group id", fields{GroupId: addr("group")}, "group_id=group"},
// 		{"message received", fields{Message: &messages{Received: &message{Offset: &offset{Topic: &top, Partition: &ptn, Offset: &off}}}}, "message.received=[offset=[topic:1:1492]]"},
// 		{"message produced", fields{Message: &messages{Produced: &message{Offset: &offset{Topic: &top, Partition: &ptn, Offset: &off}}}}, "message.produced=[offset=[topic:1:1492]]"},
// 		{"offset", fields{Offset: &offset{Topic: &top, Partition: &ptn, Offset: &off}}, "offset=[topic:1:1492]"},
// 	}
// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			f := &field{
// 				GroupId: tt.fields.GroupId,
// 				Message: tt.fields.Message,
// 				Offset:  tt.fields.Offset,
// 			}
// 			if got := f.String(); got != tt.want {
// 				t.Errorf("\nwanted %#v\ngot    %#v", tt.want, got)
// 			}
// 		})
// 	}
// }
