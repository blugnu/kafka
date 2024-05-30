package mock

import (
	"fmt"
	"slices"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type Expectation struct {
	topic   string
	header  *expectheader
	headers *expectheaders
	key     *expectbytes
	value   *expectbytes
	msg     *kafka.Message
	met     bool
	err     error
}

func (expect *Expectation) Got() string {
	return msginfo(expect.msg)
}

func (expect *Expectation) Wanted() string {
	t := "<any>"
	if expect.topic != "" {
		t = fmt.Sprintf("%q", expect.topic)
	}
	k := expect.key
	h := coalesce(expect.header.String(), expect.headers.String(), "=<any>")
	v := expect.value

	return fmt.Sprintf("topic=%s, key=%s, headers%s, value=%s", t, k, h, v)
}

func (expect *Expectation) ReturnsError(err error) *Expectation {
	expect.err = err
	return expect
}

func (expect *Expectation) WithHeader(k string) *Expectation {
	if expect.headers != nil {
		panic("WithHeaders() expectation already set (WithHeader and WithHeaders cannot both be set)")
	}
	if expect.header == nil {
		expect.header = &expectheader{}
	}
	if slices.ContainsFunc(expect.header.entries, func(h kafka.Header) bool { return h.Key == k }) {
		panic(fmt.Sprintf("expectation already set for header with key %q", k))
	}
	expect.header.entries = append(expect.header.entries, kafka.Header{Key: k})
	return expect
}

func (expect *Expectation) WithHeaderValue(k, v string) *Expectation {
	if expect.headers != nil {
		panic("WithHeaders() expectation already set (WithHeader and WithHeaders cannot both be set)")
	}
	if expect.header == nil {
		expect.header = &expectheader{}
	}
	if slices.ContainsFunc(expect.header.entries, func(h kafka.Header) bool { return h.Key == k }) {
		panic(fmt.Sprintf("expectation already set for header with key %q", k))
	}
	expect.header.entries = append(expect.header.entries, kafka.Header{Key: k, Value: []byte(v)})
	return expect
}

// WithHeaders specifies a complete set of expected headers.
//
// A message must be produced with headers matching the specified map
// in order for the expectation to be met.  Ordering of keys is not
// significant, but every key/value pair in the specified map must be
// present in the message headers.
//
// If WithHeaders is specified, WithHeader cannot also be specified.
//
// If an empty map is specified, the message must be produced with no
// headers in order for the expectation to be met.  This is equivalent
// to setting a WithNoHeaders() expectation.
func (expect *Expectation) WithHeaders(v map[string]string) *Expectation {
	if expect.headers != nil {
		panic("WithHeaders() expectation already set")
	}
	if expect.header != nil {
		panic("WithHeader() expectation already set (WithHeader and WithHeaders expectations cannot both be set)")
	}
	expect.headers = &expectheaders{v}
	return expect
}

// WithKey establishes a message Key expectation.  Returns a BytesExpectation
// that may be used to set a specific expected key value.
//
// Examples:
//
//	WithKey()              - any non-empty key
//	WithKey().Empty()      - the key must be empty
//	WithKey().Value(v)     - the key must be exactly v
//	WithKey().AsJson(v)    - the key must be exactly json.Marshal(v)
//	WithKey().AsString(v)  - the key must be exactly []byte(v)
func (expect *Expectation) WithKey() BytesExpectation {
	if expect.key != nil {
		panic("only one Key expectation may be set per message")
	}
	expect.key = &expectbytes{}
	return expect.key
}

// WithValue establishes a message Value expectation.  Returns a BytesExpectation
// that may be used to set a specific expected Value.
//
// Examples:
//
//	WithValue()              - any non-empty Value
//	WithValue().Empty()      - the Value must be empty
//	WithValue().Value(v)     - the Value must be exactly v
//	WithValue().AsJson(v)    - the Value must be exactly json.Marshal(v)
//	WithValue().AsString(v)  - the Value must be exactly []byte(v)
func (expect *Expectation) WithValue() BytesExpectation {
	if expect.value != nil {
		panic("only one Value expectation may be set per message")
	}
	expect.value = &expectbytes{}
	return expect.value
}
