package mock

import (
	"fmt"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func TestHeadersEqual(t *testing.T) {
	// ARRANGE
	testcases := []struct {
		name    string
		sut     *expectheaders
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
		{name: "expectation (empty map), no headers",
			sut:    &expectheaders{value: map[string]string{}},
			result: true,
		},
		{name: "expectation (empty map), headers",
			sut: &expectheaders{value: map[string]string{}},
			headers: []kafka.Header{
				{Key: "key", Value: []byte("value")},
			},
			result: false,
		},
		{name: "expectation (key:value), no headers",
			sut: &expectheaders{value: map[string]string{
				"key": "value",
			}},
			result: false,
		},
		{name: "expectation (key:value), headers match",
			sut: &expectheaders{value: map[string]string{
				"key": "value",
			}},
			headers: []kafka.Header{
				{Key: "key", Value: []byte("value")},
			},
			result: true,
		},
		{name: "expectation (key:value), match key not value",
			sut: &expectheaders{value: map[string]string{
				"key": "value",
			}},
			headers: []kafka.Header{
				{Key: "key", Value: []byte("other value")},
			},
			result: false,
		},
		{name: "expectation (key:value), match value not key",
			sut: &expectheaders{value: map[string]string{
				"key": "value",
			}},
			headers: []kafka.Header{
				{Key: "other key", Value: []byte("value")},
			},
			result: false,
		},
		{name: "expectation (key:value), match subset",
			sut: &expectheaders{value: map[string]string{
				"key": "value",
			}},
			headers: []kafka.Header{
				{Key: "other key", Value: []byte("value")},
				{Key: "key", Value: []byte("value")},
				{Key: "key", Value: []byte("other value")},
			},
			result: false,
		},
		{name: "expectation (multiple), match subset",
			sut: &expectheaders{value: map[string]string{
				"key.1": "value.1",
				"key.2": "value.2",
			}},
			headers: []kafka.Header{
				{Key: "key.1", Value: []byte("value.1")},
				{Key: "key.2", Value: []byte("value.2")},
				{Key: "key.3", Value: []byte("value.3")},
			},
			result: false,
		},
		{name: "expectation (multiple), match all",
			sut: &expectheaders{value: map[string]string{
				"key.1": "value.1",
				"key.2": "value.2",
				"key.3": "value.3",
			}},
			headers: []kafka.Header{
				{Key: "key.1", Value: []byte("value.1")},
				{Key: "key.2", Value: []byte("value.2")},
				{Key: "key.3", Value: []byte("value.3")},
			},
			result: true,
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			// ACT
			got := tc.sut.Equal(tc.headers)

			// ASSERT
			wanted := tc.result
			if wanted != got {
				t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
			}
		})
	}
}

func TestHeadersString(t *testing.T) {
	// ARRANGE
	testcases := []struct {
		sut    *expectheaders
		result string
	}{
		{sut: nil, result: ""},
		{sut: &expectheaders{
			value: map[string]string{
				"key": "value",
			}},
			result: "=[{\"key\": \"value\"}]",
		},
		{sut: &expectheaders{
			value: map[string]string{
				"key.1": "value.1",
				"key.2": "value.2",
			}},
			result: "=[{\"key.1\": \"value.1\"}, {\"key.2\": \"value.2\"}]",
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
