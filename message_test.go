package kafka

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/blugnu/kafka/context"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func TestUnmarshalJson(t *testing.T) {
	// ARRANGE
	ctx := context.Background()

	msg := &kafka.Message{
		Value: []byte(`{"name":"test"}`),
	}

	// ACT
	got, err := UnmarshalJson[struct{ Name string }](ctx, msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// ASSERT
	wanted := &struct{ Name string }{Name: "test"}
	if !reflect.DeepEqual(wanted, got) {
		t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
	}
}

func TestUnmarshalJsonWithInvalidJson(t *testing.T) {
	// ARRANGE
	ctx := context.Background()

	msg := &kafka.Message{
		Value: []byte(`this is not json`),
	}

	v := map[string]any{}
	jsonerr := json.Unmarshal([]byte("this is not json"), &v)

	// ACT
	_, got := UnmarshalJson[int](ctx, msg)

	// ASSERT
	// this isn't the best way to test for an expected error but it's the best I could
	// come up with given the way json.Unmarshal is implemented
	wanted := jsonerr
	if wanted.Error() != got.Error() {
		t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
	}
}
