package context

import (
	"context"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func get[T any](ctx context.Context, k key) T {
	v, _ := has[T](ctx, k)
	return v
}

func has[T any](ctx context.Context, k key) (T, bool) {
	if v, ok := ctx.Value(k).(T); ok {
		return v, true
	}
	var z T
	return z, false
}

func GroupId(ctx context.Context) (string, bool) {
	return has[string](ctx, kGroupId)
}

func MessageProduced(ctx context.Context) *kafka.Message {
	return get[*kafka.Message](ctx, kMessageProduced)
}

func MessageReceived(ctx context.Context) *kafka.Message {
	return get[*kafka.Message](ctx, kMessageReceived)
}

func Offset(ctx context.Context) *kafka.TopicPartition {
	return get[*kafka.TopicPartition](ctx, kOffset)
}

func WithGroupId(ctx context.Context, id string) Context {
	return context.WithValue(ctx, kGroupId, id)
}

func WithMessageProduced(ctx context.Context, msg *kafka.Message) context.Context {
	cp := *msg
	return context.WithValue(ctx, kMessageProduced, &cp)
}

func WithMessageReceived(ctx context.Context, msg *kafka.Message) context.Context {
	cp := *msg
	return context.WithValue(ctx, kMessageReceived, &cp)
}

func WithOffset(ctx context.Context, offset *kafka.TopicPartition) context.Context {
	cp := *offset
	return context.WithValue(ctx, kOffset, &cp)
}
