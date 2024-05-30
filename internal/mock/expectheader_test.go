package mock

import (
	"fmt"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func TestHeaderIn(t *testing.T) {
	// ARRANGE
	testcases := []struct {
		name    string
		sut     *expectheader
		headers []kafka.Header
		result  bool
	}{
		{name: "nil expectation, no headers",
			result: true,
		},
		{name: "nil expectation, headers",
			headers: []kafka.Header{
				{Key: "key", Value: []byte("value")},
			},
			result: true,
		},
		{name: "expectation (key:value), no headers",
			sut: &expectheader{entries: []kafka.Header{
				{Key: "key", Value: []byte("value")},
			}},
			result: false,
		},
		{name: "expectation (key:value), headers match",
			sut: &expectheader{entries: []kafka.Header{
				{Key: "key", Value: []byte("value")},
			}},
			headers: []kafka.Header{
				{Key: "key", Value: []byte("value")},
			},
			result: true,
		},
		{name: "expectation (key:value), match key not value",
			sut: &expectheader{entries: []kafka.Header{
				{Key: "key", Value: []byte("value")},
			}},
			headers: []kafka.Header{
				{Key: "key", Value: []byte("other value")},
			},
			result: false,
		},
		{name: "expectation (key:value), match value not key",
			sut: &expectheader{entries: []kafka.Header{
				{Key: "key", Value: []byte("value")},
			}},
			headers: []kafka.Header{
				{Key: "other key", Value: []byte("value")},
			},
			result: false,
		},
		{name: "expectation (key:value), match subset",
			sut: &expectheader{entries: []kafka.Header{
				{Key: "key", Value: []byte("value")},
			}},
			headers: []kafka.Header{
				{Key: "other key", Value: []byte("value")},
				{Key: "key", Value: []byte("value")},
				{Key: "key", Value: []byte("other value")},
			},
			result: true,
		},
		{name: "expectation (multiple), match one",
			sut: &expectheader{entries: []kafka.Header{
				{Key: "key.1", Value: []byte("value.1")},
				{Key: "key.2", Value: []byte("value.2")},
			}},
			headers: []kafka.Header{
				{Key: "key.1", Value: []byte("value.1")},
				{Key: "key.m", Value: []byte("value.m")},
				{Key: "key.n", Value: []byte("value.n")},
			},
			result: false,
		},
		{name: "expectation (multiple), match all",
			sut: &expectheader{entries: []kafka.Header{
				{Key: "key.1", Value: []byte("value.1")},
				{Key: "key.2", Value: []byte("value.2")},
			}},
			headers: []kafka.Header{
				{Key: "key.1", Value: []byte("value.1")},
				{Key: "key.2", Value: []byte("value.2")},
				{Key: "key.n", Value: []byte("value.n")},
			},
			result: true,
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			// ACT
			got := tc.sut.In(tc.headers)

			// ASSERT
			wanted := tc.result
			if wanted != got {
				t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
			}
		})
	}
}

func TestHeaderString(t *testing.T) {
	// ARRANGE
	testcases := []struct {
		sut    *expectheader
		result string
	}{
		{sut: nil, result: ""},
		{sut: &expectheader{
			entries: []kafka.Header{
				{Key: "key", Value: []byte("value")},
			}},
			result: " contain:[{\"key\": \"value\"}]",
		},
		{sut: &expectheader{
			entries: []kafka.Header{
				{Key: "key.1", Value: []byte("value.1")},
				{Key: "key.2", Value: []byte("value.2")},
			}},
			result: " contain:[{\"key.1\": \"value.1\"}, {\"key.2\": \"value.2\"}]",
		},
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
