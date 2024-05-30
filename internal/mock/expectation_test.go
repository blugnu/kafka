package mock

import (
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func TestExpectationGot(t *testing.T) {
	// ARRANGE
	topic := "topic"

	testcases := []struct {
		msg    *kafka.Message
		result string
	}{
		{result: "<no message>"},
		{msg: &kafka.Message{}, result: "topic=<none>, key=<none>, headers=<none>, value=<none>"},
		{msg: &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &topic}}, result: "topic=\"topic\", key=<none>, headers=<none>, value=<none>"},
		{msg: &kafka.Message{Key: []byte("key")}, result: "topic=<none>, key=\"key\", headers=<none>, value=<none>"},
		{msg: &kafka.Message{Value: []byte("value")}, result: "topic=<none>, key=<none>, headers=<none>, value=\"value\""},
		{msg: &kafka.Message{
			Headers: []kafka.Header{
				{Key: "key", Value: []byte("value")},
			}},
			result: "topic=<none>, key=<none>, headers=[{\"key\": \"value\"}], value=<none>",
		},
		{msg: &kafka.Message{
			Headers: []kafka.Header{
				{Key: "key.1", Value: []byte("value.1")},
				{Key: "key.2", Value: []byte("value.2")},
			}},
			result: "topic=<none>, key=<none>, headers=[{\"key.1\": \"value.1\"}, {\"key.2\": \"value.2\"}], value=<none>",
		},
	}
	for tn, tc := range testcases {
		t.Run(fmt.Sprintf("%d", tn+1), func(t *testing.T) {
			// ARRANGE
			sut := &Expectation{msg: tc.msg}

			// ACT
			got := sut.Got()

			// ASSERT
			wanted := tc.result
			if wanted != got {
				t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
			}
		})
	}
}

func TestExpectationWanted(t *testing.T) {
	// ARRANGE
	testcases := []struct {
		sut    *Expectation
		result string
	}{
		{sut: &Expectation{}, result: "topic=<any>, key=<any>, headers=<any>, value=<any>"},
		{sut: &Expectation{topic: "topic"}, result: "topic=\"topic\", key=<any>, headers=<any>, value=<any>"},
		{sut: &Expectation{key: &expectbytes{}}, result: "topic=<any>, key=\"\", headers=<any>, value=<any>"},
		{sut: &Expectation{value: &expectbytes{}}, result: "topic=<any>, key=<any>, headers=<any>, value=\"\""},
		{sut: &Expectation{
			header: &expectheader{entries: []kafka.Header{
				{Key: "key", Value: []byte("value")},
			}}},
			result: "topic=<any>, key=<any>, headers contain:[{\"key\": \"value\"}], value=<any>",
		},
		{sut: &Expectation{
			header: &expectheader{entries: []kafka.Header{
				{Key: "key.1", Value: []byte("value.1")},
				{Key: "key.2", Value: []byte("value.2")},
			}}},
			result: "topic=<any>, key=<any>, headers contain:[{\"key.1\": \"value.1\"}, {\"key.2\": \"value.2\"}], value=<any>",
		},
		{sut: &Expectation{
			headers: &expectheaders{value: map[string]string{
				"key": "value",
			}}},
			result: "topic=<any>, key=<any>, headers=[{\"key\": \"value\"}], value=<any>",
		},
		{sut: &Expectation{
			headers: &expectheaders{value: map[string]string{
				"key.1": "value.1",
				"key.2": "value.2",
			}}},
			result: "topic=<any>, key=<any>, headers=[{\"key.1\": \"value.1\"}, {\"key.2\": \"value.2\"}], value=<any>",
		},
	}
	for tn, tc := range testcases {
		t.Run(fmt.Sprintf("%d", tn+1), func(t *testing.T) {
			// ACT
			got := tc.sut.Wanted()

			// ASSERT
			wanted := tc.result
			if wanted != got {
				t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
			}
		})
	}
}

func TestReturnsError(t *testing.T) {
	// ARRANGE
	err := errors.New("error")
	sut := &Expectation{}

	// ACT
	got := sut.ReturnsError(err)

	// ASSERT
	t.Run("returns", func(t *testing.T) {
		wanted := sut
		if wanted != got {
			t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
		}
	})

	t.Run("sets error", func(t *testing.T) {
		wanted := err
		got := sut.err
		if wanted != got {
			t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
		}
	})
}

func TestWithHeader(t *testing.T) {
	// ARRANGE
	sut := &Expectation{}

	// ACT
	got := sut.WithHeader("key")

	// ASSERT
	t.Run("returns", func(t *testing.T) {
		wanted := sut
		if wanted != got {
			t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
		}
	})

	t.Run("adds header", func(t *testing.T) {
		wanted := &expectheader{entries: []kafka.Header{{Key: "key"}}}
		got := sut.header
		if !reflect.DeepEqual(wanted, got) {
			t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
		}
	})
}

func TestWithHeaderWhenKeyAlreadyExpected(t *testing.T) {
	// ARRANGE
	sut := &Expectation{
		header: &expectheader{entries: []kafka.Header{
			{Key: "key", Value: []byte("value")},
		}},
	}
	defer func() { // panic tests must be deferred
		if r := recover(); r == nil {
			t.Run("panics", func(t *testing.T) {
				t.Errorf("did not panic")
			})
		}
	}()

	// ACT
	sut.WithHeader("key")
}

func TestWithHeaderWhenHeadersAlreadyExpected(t *testing.T) {
	// ARRANGE
	sut := &Expectation{
		headers: &expectheaders{},
	}
	defer func() { // panic tests must be deferred
		if r := recover(); r == nil {
			t.Run("panics", func(t *testing.T) {
				t.Errorf("did not panic")
			})
		}
	}()

	// ACT
	sut.WithHeader("key")
}

func TestWithHeaderValue(t *testing.T) {
	// ARRANGE
	sut := &Expectation{}

	// ACT
	got := sut.WithHeaderValue("key", "value")

	// ASSERT
	t.Run("returns", func(t *testing.T) {
		wanted := sut
		if wanted != got {
			t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
		}
	})

	t.Run("adds header", func(t *testing.T) {
		wanted := &expectheader{entries: []kafka.Header{{Key: "key", Value: []byte("value")}}}
		got := sut.header
		if !reflect.DeepEqual(wanted, got) {
			t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
		}
	})
}

func TestWithHeaderValueWhenKeyAlreadyExpected(t *testing.T) {
	// ARRANGE
	sut := &Expectation{
		header: &expectheader{entries: []kafka.Header{
			{Key: "key", Value: []byte("value")},
		}},
	}
	defer func() { // panic tests must be deferred
		if r := recover(); r == nil {
			t.Run("panics", func(t *testing.T) {
				t.Errorf("did not panic")
			})
		}
	}()

	// ACT
	sut.WithHeaderValue("key", "value")
}

func TestWithHeaderValueWhenHeadersAlreadyExpected(t *testing.T) {
	// ARRANGE
	sut := &Expectation{
		headers: &expectheaders{},
	}
	defer func() { // panic tests must be deferred
		if r := recover(); r == nil {
			t.Run("panics", func(t *testing.T) {
				t.Errorf("did not panic")
			})
		}
	}()

	// ACT
	sut.WithHeaderValue("key", "value")
}

func TestWithHeaders(t *testing.T) {
	// ARRANGE
	sut := &Expectation{}

	// ACT
	got := sut.WithHeaders(map[string]string{"key": "value"})

	// ASSERT
	t.Run("returns", func(t *testing.T) {
		wanted := sut
		if wanted != got {
			t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
		}
	})

	t.Run("adds headers", func(t *testing.T) {
		wanted := &expectheaders{value: map[string]string{"key": "value"}}
		got := sut.headers
		if !reflect.DeepEqual(wanted, got) {
			t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
		}
	})
}

func TestWithHeadersWhenHeadersAlreadyExpected(t *testing.T) {
	// ARRANGE
	sut := &Expectation{
		headers: &expectheaders{},
	}
	defer func() { // panic tests must be deferred
		if r := recover(); r == nil {
			t.Run("panics", func(t *testing.T) {
				t.Errorf("did not panic")
			})
		}
	}()

	// ACT
	sut.WithHeaders(map[string]string{})
}

func TestWithHeadersWhenHeaderAlreadyExpected(t *testing.T) {
	// ARRANGE
	sut := &Expectation{
		header: &expectheader{},
	}
	defer func() { // panic tests must be deferred
		if r := recover(); r == nil {
			t.Run("panics", func(t *testing.T) {
				t.Errorf("did not panic")
			})
		}
	}()

	// ACT
	sut.WithHeaders(map[string]string{})
}
func TestWithKey(t *testing.T) {
	// ARRANGE
	sut := &Expectation{}

	// ACT
	got := sut.WithKey()

	// ASSERT
	t.Run("returns", func(t *testing.T) {
		wanted := &expectbytes{}
		if !reflect.DeepEqual(wanted, got) {
			t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
		}
	})

	t.Run("sets key expectation", func(t *testing.T) {
		wanted := expectbytes{}
		got := *sut.key
		if !reflect.DeepEqual(wanted, got) {
			t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
		}
	})
}

func TestThatWithKeyPanicsWhenAlreadySet(t *testing.T) {
	// ARRANGE
	sut := &Expectation{key: &expectbytes{}}

	defer func() { // panic tests must be deferred
		if r := recover(); r == nil {
			t.Run("panics", func(t *testing.T) {
				t.Errorf("did not panic")
			})
		}
	}()

	// ACT
	sut.WithKey()
}

func TestWithValue(t *testing.T) {
	// ARRANGE
	sut := &Expectation{}

	// ACT
	got := sut.WithValue()

	// ASSERT
	t.Run("returns", func(t *testing.T) {
		wanted := &expectbytes{}
		if !reflect.DeepEqual(wanted, got) {
			t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
		}
	})

	t.Run("sets value expectation", func(t *testing.T) {
		wanted := expectbytes{}
		got := *sut.value
		if !reflect.DeepEqual(wanted, got) {
			t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
		}
	})
}

func TestThatWithValuePanicsWhenAlreadySet(t *testing.T) {
	// ARRANGE
	sut := &Expectation{value: &expectbytes{}}

	defer func() { // panic tests must be deferred
		if r := recover(); r == nil {
			t.Run("panics", func(t *testing.T) {
				t.Errorf("did not panic")
			})
		}
	}()

	// ACT
	sut.WithValue()
}
