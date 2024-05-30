package mock

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
)

func TestBytesAsJson(t *testing.T) {
	// ARRANGE
	type id struct {
		Id string
	}

	testcases := []struct {
		name   string
		value  *id
		result *expectbytes
	}{
		{name: "nil value", result: &expectbytes{value: []byte("null")}},
		{name: "non-nil value", value: &id{"id"}, result: &expectbytes{value: []byte("{\"Id\":\"id\"}")}},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			// ARRANGE
			sut := &expectbytes{}

			// ACT
			sut.AsJson(tc.value)

			// ASSERT
			wanted := tc.result
			got := sut
			if !reflect.DeepEqual(wanted, got) {
				t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
			}
		})
	}
}

type unmarshallable int

func (u unmarshallable) MarshalJSON() ([]byte, error) {
	return nil, errors.New("marshalling error")
}

func TestBytesAsJsonPanicsWhenMarshalFails(t *testing.T) {
	// ARRANGE
	defer func() { // panic tests must be deferred
		if r := recover(); r == nil {
			t.Run("panics", func(t *testing.T) {
				t.Errorf("did not panic")
			})
		}
	}()

	// ARRANGE
	sut := &expectbytes{}

	// ACT
	sut.AsJson(unmarshallable(1))
}

func TestBytesAsString(t *testing.T) {
	// ARRANGE
	sut := &expectbytes{}

	// ACT
	sut.AsString("some string")

	// ASSERT
	wanted := &expectbytes{value: []byte("some string")}
	got := sut
	if !reflect.DeepEqual(wanted, got) {
		t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
	}
}

func TestBytesEmpty(t *testing.T) {
	// ARRANGE
	sut := &expectbytes{}

	// ACT
	sut.Empty()

	// ASSERT
	wanted := &expectbytes{value: []byte{}}
	got := sut
	if !reflect.DeepEqual(wanted, got) {
		t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
	}
}

func TestBytesEqual(t *testing.T) {
	// ARRANGE
	testcases := []struct {
		name   string
		sut    *expectbytes
		bytes  []byte
		result bool
	}{
		{name: "no expectation (nil), empty value", result: true},
		{name: "no expectation (nil), non-empty value", bytes: []byte("any"), result: true},
		{name: "expectation with nil bytes, nil value", sut: &expectbytes{}, result: false},
		{name: "expectation with nil bytes, empty value", sut: &expectbytes{}, bytes: []byte{}, result: true},
		{name: "expectation with nil bytes, non-empty value", sut: &expectbytes{}, bytes: []byte("any"), result: true},
		{name: "expectation (empty), empty value", sut: &expectbytes{[]byte{}}, result: true},
		{name: "expectation (empty), non-empty value", sut: &expectbytes{[]byte{}}, bytes: []byte("any"), result: false},
		{name: "expectation (non-empty), unequal value", sut: &expectbytes{value: []byte("expect")}, bytes: []byte("not this"), result: false},
		{name: "expectation (non-empty), equal value", sut: &expectbytes{value: []byte("this")}, bytes: []byte("this"), result: true},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			// ACT
			got := tc.sut.Equal(tc.bytes)

			// ASSERT
			wanted := tc.result
			if wanted != got {
				t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
			}
		})
	}
}

func TestBytesString(t *testing.T) {
	// ARRANGE
	testcases := []struct {
		sut    *expectbytes
		result string
	}{
		{sut: nil, result: "<any>"},
		{sut: &expectbytes{}, result: "\"\""},
		{sut: &expectbytes{[]byte("")}, result: "\"\""},
		{sut: &expectbytes{[]byte("any")}, result: "\"any\""},
	}
	for tn, tc := range testcases {
		t.Run(fmt.Sprintf("%d", tn), func(t *testing.T) {
			// ACT
			got := tc.sut.String()

			// ASSERT
			wanted := tc.result
			if wanted != got {
				t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
			}
		})
	}

}

func TestBytesValue(t *testing.T) {
	// ARRANGE
	sut := &expectbytes{}

	// ACT
	sut.Value([]byte("bytes"))

	// ASSERT
	wanted := &expectbytes{value: []byte("bytes")}
	got := sut
	if !reflect.DeepEqual(wanted, got) {
		t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
	}
}
