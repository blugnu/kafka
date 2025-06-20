package mock

import (
	"context"
	"errors"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// Producer provides a mock implementation of kafka.Producer.
//
// Since a Producer provides only a MustProduce() method, all
// expectations start with an expectation that some message will be
// produced to a particular topic.
//
// The details of the expected message may then optionally be
// specified using a fluent api.  If no details are specified, then
// any message to the topic will satisfy the expectation.  Any
// details specified must all match those of the message produced
// for the expectation to be met:
//
// Example: expecting any message to be produced to a topic:
//
//	p := &mock.Producer{}
//	p.Expect("topic")
//
// Example: expecting a message with a particular Header:
//
//	p := &mock.Producer{}
//	p.Expect("topic").
//		WithHeader("header.key", "value")
//
// If multiple messages are expected, the expectations are met only
// if messages are produced which meet the specified expectations in
// the order in which the expectations were set.
//
// Example: expecting two messages to be produced where the first
// has a specific key and the second could be any message produced
// to a different topic:
//
//	p := &mock.Producer{}
//	p.Expect("topic.a").
//		WithKey(customerId)
//	p.Expect("topic.b")
//
// If testing the behaviour of higher level code in response to producer
// errors, you may set an expectation that an attempt to produce a message
// will return an error.
//
// Example: mocking a producer error:
//
//	p := &mock.Producer{}
//	p.Expect("topic").ReturnsError(errors.New("producer error"))
//
// To test whether expectations were met call ExpectationsMet() after
// the code under test has completed.  If any expectations were not met
// false is returned with an error that describes the unmet expectations.
// If all expectations were met, true is returned with a nil error.
//
// The bool indicator is used to control test flow and provide
// your own test failure report, ignoring the error:
//
//	if met, _ := p.ExpectationsMet(); !met {
//		t.Error("did not produce 'completed' event")
//	}
//
// or you can ignore the bool indicator and simply report a non-nil error:
//
//	if _, err := p.ExpectationsMet(); err != nil {
//		t.Error(err.Error())
//	}
//
// To re-use a mock.Producer in multiple executions of a test (e.g. when
// using a data-driven test), the Reset() method may be used to reset the
// mock.Producer to its initial state, clearing all expectations.
type Producer[T comparable] struct {
	expectations []*Expectation
	unexpected   []*kafka.Message
	messages     map[string][]*kafka.Message
	next         int
	err          error
}

// Err returns the error that was set on the Producer.
func (p *Producer[T]) Err() error {
	return p.err
}

// Expect sets an expectation that a message will be produced to the
// specified topic.  The expectation may be further specified using the
// fluent API provided by the returned Expectation.
func (p *Producer[T]) Expect(topic T) *Expectation {
	ex := &Expectation{topic: fmt.Sprintf("%v", topic)}
	p.expectations = append(p.expectations, ex)
	return ex
}

// ExpectationsMet checks whether all expectations set on the Producer were met.
func (p *Producer[T]) ExpectationsWereMet() error {
	errs := []error{}
	for i, ex := range p.expectations {
		if !ex.met {
			errs = append(errs, fmt.Errorf("\nexpectation %d not met:\n  expected : %s\n  got      : %s", i+1, ex.Wanted(), ex.Got()))
		}
	}
	for _, msg := range p.unexpected {
		errs = append(errs, fmt.Errorf("produced unexpected message: %s", msginfo(msg)))
	}
	if err := errors.Join(errs...); err != nil {
		return err
	}
	return nil
}

// produce simulates the production of a message to the specified topic.
//
// If the topic is not set in the message, it sets a partition error indicating that the topic
// is not set, otherwise the topic partition is initialized and the message is appended
// to the messages map for that topic.
func (p *Producer[T]) produce(msg kafka.Message) *kafka.TopicPartition {
	tp := &kafka.TopicPartition{
		Topic:     msg.TopicPartition.Topic,
		Partition: 0,
	}

	if msg.TopicPartition.Topic == nil {
		tp.Error = ErrTopicNotSet
		return tp
	}

	topic := *msg.TopicPartition.Topic
	tp.Offset = kafka.Offset(len(p.messages[topic]))
	tp.Error = nil

	p.messages[topic] = append(p.messages[topic], &msg)

	return tp
}

// MustProduce produces a message to the topic specified in the expectation.
func (p *Producer[T]) MustProduce(ctx context.Context, msg kafka.Message) (*kafka.TopicPartition, error) {
	if p.next == 0 {
		p.messages = map[string][]*kafka.Message{}
	}

	tp := p.produce(msg)

	if p.next == len(p.expectations) {
		p.unexpected = append(p.unexpected, &msg)
		return tp, coalesce(tp.Error, p.err)
	}

	ex := p.expectations[p.next]
	p.next++
	p.err = ex.err

	ex.msg = &msg
	if ex.err != nil {
		ex.met = true
		return nil, ex.err
	}

	ex.met = (ex.topic == "" || (msg.TopicPartition.Topic != nil && ex.topic == *msg.TopicPartition.Topic))
	if !ex.met {
		return nil, fmt.Errorf("message produced to unexpected topic\n  wanted: %s\n  got   : %s", ex.topic, *msg.TopicPartition.Topic)
	}

	ex.met = ex.key.Equal(msg.Key) &&
		ex.value.Equal(msg.Value) &&
		ex.headers.Equal(msg.Headers) &&
		ex.header.In(msg.Headers)

	return tp, coalesce(tp.Error, p.err)
}

// Reset resets the Producer to its initial state, clearing all expectations
// and unexpected messages, and resetting the next index and error.
func (p *Producer[T]) Reset() {
	*p = Producer[T]{}
}
