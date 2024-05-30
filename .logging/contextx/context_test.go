package context

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func testGetter[T comparable](t *testing.T, ctx context.Context, sut func(context.Context) T, k key, value T) {
	// ARRANGE
	var z T
	if value != z {
		ctx = context.WithValue(ctx, k, value)
	}

	// ACT
	got := sut(ctx)

	// ASSERT
	t.Run("returns", func(t *testing.T) {
		wanted := value
		if wanted != got {
			t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
		}
	})
}

func testGetterWithIndicator[T comparable](t *testing.T, ctx context.Context, sut func(context.Context) (T, bool), k key, value T, ind bool) {
	// ARRANGE
	var z T
	if value != z {
		ctx = context.WithValue(ctx, k, value)
	}

	// ACT
	result, isSet := sut(ctx)

	// ASSERT
	t.Run("returns value", func(t *testing.T) {
		wanted := value
		got := result
		if wanted != got {
			t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
		}
	})

	t.Run("returns indicator", func(t *testing.T) {
		wanted := ind
		got := isSet
		if wanted != got {
			t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
		}
	})
}

func TestGroupId(t *testing.T) {
	// ARRANGE
	ctx := context.Background()

	t.Run("when not set", func(t *testing.T) {
		testGetterWithIndicator[string](t, ctx, GroupId, kGroupId, "", false)
	})

	t.Run("when set", func(t *testing.T) {
		testGetterWithIndicator[string](t, ctx, GroupId, kGroupId, "group.id", true)
	})
}

func TestMessageProduced(t *testing.T) {
	ctx := context.Background()

	t.Run("when not set", func(t *testing.T) {
		testGetter[*kafka.Message](t, ctx, MessageProduced, kMessageProduced, nil)
	})
	t.Run("when set", func(t *testing.T) {
		msg := &kafka.Message{TopicPartition: kafka.TopicPartition{Partition: kafka.PartitionAny, Offset: kafka.OffsetEnd}, Key: []byte("key")}
		testGetter[*kafka.Message](t, ctx, MessageProduced, kMessageProduced, msg)
	})
}

func TestMessageReeived(t *testing.T) {
	ctx := context.Background()

	t.Run("when not set", func(t *testing.T) {
		testGetter[*kafka.Message](t, ctx, MessageReceived, kMessageReceived, nil)
	})
	t.Run("when set", func(t *testing.T) {
		msg := &kafka.Message{}
		testGetter[*kafka.Message](t, ctx, MessageReceived, kMessageReceived, msg)
	})
}

func TestOffset(t *testing.T) {
	ctx := context.Background()

	t.Run("when not set", func(t *testing.T) {
		testGetter[*kafka.TopicPartition](t, ctx, Offset, kOffset, nil)
	})
	t.Run("when set", func(t *testing.T) {
		off := &kafka.TopicPartition{}
		testGetter[*kafka.TopicPartition](t, ctx, Offset, kOffset, off)
	})
}

func TestWithGroupId(t *testing.T) {
	// ARRANGE
	ctx := context.Background()

	// ACT
	ctx = WithGroupId(ctx, "group.id")

	// ASSERT
	wanted := "group.id"
	got := ctx.Value(kGroupId)
	if wanted != got {
		t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
	}
}

func TestWithMessageProduced(t *testing.T) {
	// ARRANGE
	ctx := context.Background()
	tpc := "topic"
	ts := time.Now()
	msg := &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &tpc, Partition: kafka.PartitionAny}, Timestamp: ts}

	// ACT
	sut := WithMessageProduced(ctx, msg)

	// ASSERT
	wanted := msg
	got := sut.Value(kMessageProduced)
	if !reflect.DeepEqual(wanted, got) {
		t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
	}
}

func TestWithMessageReceived(t *testing.T) {
	// ARRANGE
	ctx := context.Background()
	msg := &kafka.Message{TopicPartition: kafka.TopicPartition{Partition: 1, Offset: 1492}, Key: []byte("key")}

	// ACT
	sut := WithMessageReceived(ctx, msg)

	// ASSERT
	wanted := msg
	got := sut.Value(kMessageReceived)
	if !reflect.DeepEqual(wanted, got) {
		t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
	}
}

func TestWithOffset(t *testing.T) {
	// ARRANGE
	ctx := context.Background()
	tpa := &kafka.TopicPartition{Partition: 1, Offset: 1492}

	// ACT
	sut := WithOffset(ctx, tpa)

	// ASSERT
	wanted := tpa
	got := sut.Value(kOffset)
	if !reflect.DeepEqual(wanted, got) {
		t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
	}
}
