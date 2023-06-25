package kafka

import (
	"context"
	"errors"
	"testing"

	"github.com/blugnu/kafka/mock"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func TestRetrySetsInitialRetryCount(t *testing.T) {
	// ARRANGE
	topic := "topic"

	ctx := context.Background()
	msg := &Message{TopicPartition: kafka.TopicPartition{Topic: &topic}}
	err := errors.New("handler error")
	prod := &mock.Producer[string]{}

	sut := &Retry[string]{
		MetadataHandler: &retrymetadatahandler{},
		Throttling:      &RetryThrottle{MaxRetries: 10},
		consumer: &consumer{
			Producer: prod,
		},
	}

	prod.Expect("topic").
		WithHeaderValue("retry-seq", "1").
		WithHeaderValue("retry-num", "1").
		WithHeaderValue("retry-max", "10")

	// ACT
	err = sut.HandleError(ctx, msg, err)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// ASSERT
	if _, err = prod.ExpectationsMet(); err != nil {
		t.Error(err)
	}
}

func TestRetryIncrementsRetryCount(t *testing.T) {
	// ARRANGE
	topic := "topic"

	ctx := context.Background()
	msg := &Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic},
		Headers: []kafka.Header{
			{Key: "retry-seq", Value: []byte("1")},
			{Key: "retry-num", Value: []byte("1")},
			{Key: "retry-max", Value: []byte("5")},
			{Key: "retry-interval", Value: []byte("1s")},
		},
	}
	err := errors.New("handler error")
	prod := &mock.Producer[string]{}

	sut := &Retry[string]{
		MetadataHandler: &retrymetadatahandler{},
		Throttling:      &RetryThrottle{},
		consumer: &consumer{
			Producer: prod,
		},
	}

	prod.Expect("topic").
		WithHeaderValue("retry-seq", "1").
		WithHeaderValue("retry-num", "2").
		WithHeaderValue("retry-max", "5").
		WithHeaderValue("retry-interval", "1s")

	// ACT
	err = sut.HandleError(ctx, msg, err)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// ASSERT
	if _, err = prod.ExpectationsMet(); err != nil {
		t.Error(err)
	}
}

func TestRetryReturnsThrottlingError(t *testing.T) {
	// ARRANGE
	topic := "topic"

	ctx := context.Background()
	msg := &Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic},
		Headers: []kafka.Header{
			{Key: "retry-seq", Value: []byte("1")},
			{Key: "retry-num", Value: []byte("5")},
			{Key: "retry-max", Value: []byte("5")},
		},
	}
	err := errors.New("handler error")
	prod := &mock.Producer[string]{}

	sut := &Retry[string]{
		MetadataHandler: &retrymetadatahandler{},
		Throttling:      &RetryThrottle{},
		consumer: &consumer{
			Producer: prod,
		},
	}

	// ACT
	got := sut.HandleError(ctx, msg, err)
	wanted := ErrRetryLimitReached
	if !errors.Is(got, wanted) {
		t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
	}

	// ASSERT
	if _, err = prod.ExpectationsMet(); err != nil {
		t.Error(err)
	}
}

func TestRetryReturnsProducerError(t *testing.T) {
	// ARRANGE
	topic := "topic"

	ctx := context.Background()
	msg := &Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic},
	}
	err := errors.New("handler error")
	proderr := errors.New("producer error")

	prod := &mock.Producer[string]{}
	prod.Expect("topic").ReturnsError(proderr)

	sut := &Retry[string]{
		MetadataHandler: &retrymetadatahandler{},
		Throttling:      &RetryThrottle{MaxRetries: 1},
		consumer: &consumer{
			Producer: prod,
		},
	}

	// ACT
	got := sut.HandleError(ctx, msg, err)
	wanted := proderr
	if !errors.Is(got, wanted) {
		t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
	}

	// ASSERT
	if _, err = prod.ExpectationsMet(); err != nil {
		t.Error(err)
	}
}
